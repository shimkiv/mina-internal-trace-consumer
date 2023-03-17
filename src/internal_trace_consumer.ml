open Core
open Async
module Block_tracing = Block_tracing

(* TODO: when current block is "0", checkpoints and metadata must be discarded *)
let current_block = ref ""

(* TODO: cleanup this from time to time *)
let block_switches = ref []

let block_just_switched = ref false

let pending_prover_checkpoints = ref []

let pending_verifier_checkpoints = ref []

let find_nearest_block timestamp =
  List.find_map !block_switches ~f:(fun (block, switch_timestamp) ->
      if Float.(timestamp >= switch_timestamp) then Some block else None )

let add_checkpoint_to_closest_trace (checkpoint, timestamp) =
  match find_nearest_block timestamp with
  | None ->
      true
  | Some block_id ->
      Block_tracing.record ~block_id ~checkpoint ~timestamp ~ordered:true () ;
      false

let process_pending_checkpoints pending_checkpoints =
  if not @@ List.is_empty !pending_checkpoints then
    pending_checkpoints :=
      !pending_checkpoints |> List.rev
      |> List.filter ~f:add_checkpoint_to_closest_trace
      |> List.rev

module Main_handler = struct
  let process_checkpoint checkpoint timestamp =
    if !block_just_switched then
      block_switches := (!current_block, timestamp) :: !block_switches ;
    Block_tracing.record ~block_id:!current_block ~checkpoint ~timestamp ()

  let process_control control data =
    match control with
    | "current_block" ->
        current_block := Yojson.Safe.Util.to_string data ;
        block_just_switched := true
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

  (* TODO: reset all traces too? *)
  let file_changed () =
    current_block := "" ;
    block_switches := []

  let eof_reached () =
    process_pending_checkpoints pending_prover_checkpoints ;
    process_pending_checkpoints pending_verifier_checkpoints
end

module Prover_handler = struct
  let process_checkpoint checkpoint timestamp =
    pending_prover_checkpoints :=
      (checkpoint, timestamp) :: !pending_prover_checkpoints

  let process_control _ _ = () (* TODO: keep pointer to previous checkpoint? *)

  let file_changed () = () (* TODO: reset state? *)

  let eof_reached () = () (* Nothing to do *)
end

module Verifier_handler = struct
  let process_checkpoint checkpoint timestamp =
    pending_verifier_checkpoints :=
      (checkpoint, timestamp) :: !pending_verifier_checkpoints

  let process_control _ _ = () (* TODO: keep pointer to previous checkpoint? *)

  let file_changed () = () (* TODO: reset state? *)

  let eof_reached () = () (* Nothing to do *)
end

module Main_trace_processor = Trace_file_processor.Make (Main_handler)
module Prover_trace_processor = Trace_file_processor.Make (Prover_handler)
module Verifier_trace_processor = Trace_file_processor.Make (Verifier_handler)

let add_filename_prefix original_path ~prefix =
  let open Filename in
  concat (dirname original_path) (prefix ^ basename original_path)

let serve =
  Command.async ~summary:"Internal trace processor with GraphQL server"
    (let%map_open.Command port =
       flag "--port" ~aliases:[ "port" ]
         (optional_with_default 9080 int)
         ~doc:"Port for GraphQL server to listen on (default 9080)"
     and main_trace_file_path =
       flag "--trace-file" ~aliases:[ "trace-file" ] (required string)
         ~doc:"Path to main internal trace file"
     in
     let prover_trace_file_path =
       add_filename_prefix main_trace_file_path ~prefix:"prover-"
     in
     let verifier_trace_file_path =
       add_filename_prefix main_trace_file_path ~prefix:"verifier-"
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
       printf "Consuming main trace events from file: %s\n%!"
         main_trace_file_path ;
       printf "Consuming prover trace events from file: %s\n%!"
         prover_trace_file_path ;
       printf "Consuming verifier trace events from file: %s\n%!"
         verifier_trace_file_path ;
       let%bind () = Main_trace_processor.process_file main_trace_file_path
       and () = Prover_trace_processor.process_file prover_trace_file_path
       and () =
         Verifier_trace_processor.process_file verifier_trace_file_path
       in
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
