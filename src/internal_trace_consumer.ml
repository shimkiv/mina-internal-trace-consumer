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

let rec process_reader ~rotated reader =
  let%bind next_line =
    Reader.really_read_line ~wait_time:(Time.Span.of_sec 0.5) reader
  in
  match next_line with
  | Some line ->
      if process_line ~rotated line then process_reader ~rotated reader
      else Deferred.unit
  | None ->
      Deferred.unit

let process_file filename =
  let rec loop rotated =
    let%bind result =
      try_with (fun () ->
          Reader.with_file filename ~f:(process_reader ~rotated) )
    in
    match result with
    | Ok () ->
        printf "File rotated, re-opening...\n%!" ;
        let%bind () = Clock.after (Time.Span.of_sec 2.0) in
        loop true
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
         ~doc:"Parth to internal trace file"
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
