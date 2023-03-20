open Core
open Async
module Block_tracing = Block_tracing

(* TODO: when current block is "0", checkpoints and metadata must be discarded *)
let current_block = ref ""

(* TODO: cleanup these from time to time *)
let verifier_calls_context_block = ref []

let prover_calls_context_block = ref []

let block_just_switched = ref false

module Pending = struct
  type t = Checkpoint of (string * float) | Control of (string * Yojson.Safe.t)
end

let pending_prover_entries : Pending.t list ref = ref []

let pending_verifier_entries : Pending.t list ref = ref []

let find_nearest_block ~context_blocks timestamp =
  List.find_map context_blocks ~f:(fun (block, switch_timestamp) ->
      if Float.(timestamp >= switch_timestamp) then Some block else None )

let current_prover_entry = ref (Block_trace.Entry.make ~timestamp:0.0 "")

let current_verifier_entry = ref (Block_trace.Entry.make ~timestamp:0.0 "")

let push_kimchi_checkpoints_from_metadata ~block_id (metadata : Yojson.Safe.t) =
  try
    let checkpoints =
      let open Yojson.Safe.Util in
      metadata |> member "traces" |> to_string |> String.split_lines
      |> List.map ~f:Yojson.Safe.from_string
    in
    let checkpoints =
      List.map checkpoints ~f:(fun json ->
          match json with
          | `List [ `String checkpoint; `Float timestamp ] ->
              `Checkpoint (checkpoint, timestamp)
          | `Assoc metadata ->
              `Metadata metadata
          | _ ->
              failwith "got malformed kimchi checkpoints" )
    in
    let current_entry =
      ref (Block_trace.Entry.make ~timestamp:0.0 "placeholder")
    in
    List.iter checkpoints ~f:(function
      | `Checkpoint (checkpoint, timestamp) ->
          let checkpoint = "Kimchi_" ^ checkpoint in
          current_entry :=
            Block_tracing.record ~block_id ~checkpoint ~timestamp ~ordered:true
              ()
      | `Metadata metadata ->
          !current_entry.metadata <- `Assoc metadata )
  with exn ->
    eprintf "[WARN] failed to integrate kimchi checkpoints: %s\n%!"
      (Exn.to_string exn) ;
    eprintf "BACKTRACE:\n%s\n%!" (Printexc.get_backtrace ())

let add_entry_to_closest_trace ~context_blocks current_entry entry =
  match entry with
  | Pending.Checkpoint (checkpoint, timestamp) -> (
      match find_nearest_block ~context_blocks timestamp with
      | None ->
          true
      | Some block_id ->
          current_entry :=
            Block_tracing.record ~block_id ~checkpoint ~timestamp ~ordered:true
              () ;
          false )
  | Pending.Control ("metadata", data) -> (
      match find_nearest_block ~context_blocks !current_entry.started_at with
      | Some block_id ->
          if
            String.equal "Backend_tick_proof_create_async"
              !current_entry.checkpoint
            || String.equal "Backend_tock_proof_create_async"
                 !current_entry.checkpoint
          then push_kimchi_checkpoints_from_metadata ~block_id data
          else
            !current_entry.metadata <-
              Yojson.Safe.Util.combine !current_entry.metadata data ;
          false
      | None ->
          true )
  | Pending.Control (_, _) ->
      (* printf "Ignoring control %s\n%!" other ; *)
      false

let process_pending_entries ~context_blocks current_entry pending_checkpoints =
  if not @@ List.is_empty !pending_checkpoints then
    pending_checkpoints :=
      !pending_checkpoints |> List.rev
      |> List.filter
           ~f:(add_entry_to_closest_trace ~context_blocks current_entry)
      |> List.rev

module Main_handler = struct
  let handle_verifier_and_prover_call_checkpoints checkpoint timestamp =
    match checkpoint with
    | "Verify_transaction_snarks"
    | "Verify_blockchain_snarks"
    | "Verify_commands" ->
        verifier_calls_context_block :=
          (!current_block, timestamp) :: !verifier_calls_context_block
    | "Produce_state_transition_proof" ->
        prover_calls_context_block :=
          (!current_block, timestamp) :: !prover_calls_context_block
    | _ ->
        ()

  let process_checkpoint checkpoint timestamp =
    let _entry =
      Block_tracing.record ~block_id:!current_block ~checkpoint ~timestamp ()
    in
    handle_verifier_and_prover_call_checkpoints checkpoint timestamp ;
    ()

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
    verifier_calls_context_block := [] ;
    prover_calls_context_block := []

  let eof_reached () =
    process_pending_entries
      ~context_blocks:!prover_calls_context_block
      current_prover_entry pending_prover_entries ;
    process_pending_entries
      ~context_blocks:!verifier_calls_context_block
      current_verifier_entry pending_verifier_entries
end

module Prover_handler = struct
  let process_checkpoint checkpoint timestamp =
    pending_prover_entries :=
      Pending.Checkpoint (checkpoint, timestamp) :: !pending_prover_entries

  let process_control tag data =
    pending_prover_entries :=
      Pending.Control (tag, data) :: !pending_prover_entries

  let file_changed () = pending_prover_entries := []

  let eof_reached () = () (* Nothing to do *)
end

module Verifier_handler = struct
  let process_checkpoint checkpoint timestamp =
    pending_verifier_entries :=
      Pending.Checkpoint (checkpoint, timestamp) :: !pending_verifier_entries

  let process_control tag data =
    pending_verifier_entries :=
      Pending.Control (tag, data) :: !pending_verifier_entries

  let file_changed () = pending_verifier_entries := []

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
