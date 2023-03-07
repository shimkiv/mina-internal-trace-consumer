open Core
open Async
module Block_tracing = Block_tracing

let current_block = ref ""

let last_rotate_end_timestamp = ref 0.0

let process_checkpoint checkpoint timestamp =
  Block_tracing.record ~block_id:!current_block ~checkpoint ~timestamp ()

let process_control control data =
  match control with
  | "current_block" ->
      current_block := Yojson.Safe.Util.to_string data
  | "metadata" ->
      Block_tracing.push_metadata ~block_id:!current_block
        (Yojson.Safe.Util.to_assoc data)
  | "block_metadata" ->
      Block_tracing.push_global_metadata ~block_id:!current_block
        (Yojson.Safe.Util.to_assoc data)
  | "produced_block_state_hash" ->
      Block_tracing.set_produced_block_state_hash ~block_id:!current_block
        (Yojson.Safe.Util.to_string data)
  | "internal_tracing_enabled" ->
      ()
  | "mina_node_metadata" ->
      ()
  | another ->
      eprintf "[WARN] unprocessed control: %s\n%!" another

let process_event original yojson =
  match yojson with
  | `List [ `String checkpoint; `Float timestamp ] ->
      process_checkpoint checkpoint timestamp ;
      true
  | `Assoc [ ("rotated_log_end", `Float timestamp) ] ->
      last_rotate_end_timestamp := timestamp ;
      false
  | `Assoc [ (head, data) ] ->
      process_control head data ; true
  | _ ->
      eprintf "[WARN] unexpected: %s\n%!" original ;
      true

let process_log_rotated_start original yojson =
  match yojson with
  | `Assoc [ ("rotated_log_start", `Float timestamp) ] ->
      if Float.(timestamp >= !last_rotate_end_timestamp) then true
      else (
        eprintf "[WARN] file rotatation issued but file didn't rotate\n%!" ;
        false )
  | _ ->
      eprintf "[WARN] expected rotated_log_start, but got: %s\n%!" original ;
      false

let process_line ~rotated line =
  try
    let yojson = Yojson.Safe.from_string line in
    if rotated then process_log_rotated_start line yojson
    else process_event line yojson
  with _ ->
    eprintf "[ERROR] could not parse line: %s\n%!" line ;
    true

let file_changed inode filename =
  try
    let stat = Core.Unix.stat filename in
    inode <> stat.st_ino
  with Unix.Unix_error _ ->
    eprintf "File '%s' removed\n%!" filename ;
    true

let really_read_line ~inode ~filename ~wait_time reader =
  let pending = ref "" in
  let rec loop () =
    let%bind result = Reader.read_until reader (`Char '\n') ~keep_delim:false in
    match result with
    | `Eof ->
        if file_changed inode filename then return `File_changed
        else
          let%bind () = Clock.after wait_time in
          loop ()
    | `Eof_without_delim data ->
        pending := !pending ^ data ;
        let%bind () = Clock.after wait_time in
        loop ()
    | `Ok line ->
        let line = !pending ^ line in
        pending := "" ;
        return (`Line line)
  in
  loop ()

let rec process_reader ~inode ~rotated ~filename reader =
  let%bind next_line =
    really_read_line ~inode ~filename ~wait_time:(Time.Span.of_sec 0.2) reader
  in
  match next_line with
  | `Line line ->
      if process_line ~rotated line then
        process_reader ~inode ~rotated ~filename reader
      else return `File_rotated
  | `File_changed ->
      (* TODO: must reset all other state too? *)
      current_block := "" ;
      last_rotate_end_timestamp := 0.0 ;
      return `File_changed

let process_file filename =
  let rec loop rotated =
    let%bind result =
      try_with (fun () ->
          Reader.with_file filename
            ~f:
              (process_reader ~inode:(Core.Unix.stat filename).st_ino ~rotated
                 ~filename ) )
    in
    match result with
    | Ok `File_rotated ->
        printf "File rotated, re-opening...\n%!" ;
        let%bind () = Clock.after (Time.Span.of_sec 2.0) in
        loop true
    | Ok `File_changed ->
        printf "File changed, re-opening...\n%!" ;
        let%bind () = Clock.after (Time.Span.of_sec 2.0) in
        loop false
    | Error exn ->
        eprintf
          "File '%s' could not be opened, retrying after 5 seconds. Reason:\n\
           %s\n\
           %!"
          filename (Exn.to_string exn) ;
        let%bind () = Clock.after (Time.Span.of_sec 5.0) in
        loop rotated
  in
  loop false

let serve =
  Command.async ~summary:"Internal trace processor with GraphQL server"
    (let%map_open.Command port =
       flag "--port" ~aliases:[ "port" ]
         (optional_with_default 9080 int)
         ~doc:"Port for GraphQL server to listen on (default 9080)"
     and filename =
       flag "--trace-file" ~aliases:[ "trace-file" ] (required string)
         ~doc:"Path to internal trace file"
     in
     fun () ->
       let insecure_rest_server = true in
       printf "Starting server on port %d...\n%!" port ;
       let%bind _ =
         Graphql_server.create_graphql_server
           ~bind_to_address:
             Tcp.Bind_to_address.(
               if insecure_rest_server then All_addresses else Localhost )
           ~schema:Graphql_server.schema ~server_description:"GraphQL server"
           port
       in
       printf "Consuming events from file: %s\n%!" filename ;
       let%bind () = process_file filename in
       printf "Done\n%!" ; Deferred.unit )

let commands = [ ("serve", serve) ]

let () =
  Async.Signal.handle [ Async.Signal.term; Async.Signal.int ] ~f:(fun _ ->
      Core.print_endline "SIGTERM/SIGINT received, exiting" ;
      Core.exit 0 ) ;
  Command.run
    (Command.group ~summary:"Internal trace processor"
       ~preserve_subcommand_order:() commands ) ;
  Core.exit 0
