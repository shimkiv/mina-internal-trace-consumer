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
    let%bind () = Reader.with_file filename ~f:(process_reader ~rotated) in
    printf "File rotated, re-opening...\n%!" ;
    let%bind () = Clock.after (Time.Span.of_sec 2.0) in
    (* TODO: take rotate end timestamp, and rotate start timestamp of new file and make
       sure it is increasing *)
    loop true
  in
  loop false

let filename_param = Command.Param.(anon ("filename" %: string))

let port_param = Command.Param.(anon ("filename" %: int))

let run =
  Command.async ~summary:"Internal trace processor"
    (Command.Param.map2 filename_param port_param ~f:(fun filename port () ->
         let%bind () = Deferred.return () in
         let insecure_rest_server = true in
         let%bind _ =
           Graphql_server.create_graphql_server
             ~bind_to_address:
               Tcp.Bind_to_address.(
                 if insecure_rest_server then All_addresses else Localhost )
             ~schema:Graphql_server.schema ~server_description:"GraphQL server"
             port
         in
         printf "Consuming events from file: %s\n%!" filename ;
         printf "Starting server on port %d...\n%!" port ;
         let%bind () = process_file filename in
         printf "Done\n%!" ; Deferred.unit ) )

let commands = [ ("run", run) ]

let () =
  Command.run
    (Command.group ~summary:"Internal trace processor"
       ~preserve_subcommand_order:() commands ) ;
  Core.exit 0
