open Core
open Async
module Block_tracing = Block_tracing

(* TODO: when current block is "0", checkpoints and metadata must be discarded *)
let current_block = ref "0"

(* TODO: cleanup these from time to time *)

(* These tables keep track of the context switches for blocks in the main trace when
   making calls to the prover or verifier. The mapping is from:
   [parent_call_checkpoint -> (block_id, timestamp)] *)
let verifier_calls_context_block : (string * float) list String.Table.t =
  String.Table.create ()

let prover_calls_context_block : (string * float) list String.Table.t =
  String.Table.create ()

module Pending = struct
  type t = Checkpoint of (string * float) | Control of (string * Yojson.Safe.t)

  type registry = t list Int.Table.t

  let prover_entries : registry = Int.Table.create ()

  let verifier_entries : registry = Int.Table.create ()

  (* Helper for being able to find the location in the main trace on which to attach
     verifier and prover checkpoints. Also to know when a verifier/prover trace is
     already complete (partial traces are left pending until completed) *)
  let parent_and_finish_checkpoints = function
    | "Verifier_verify_transaction_snarks" ->
        Some
          ( "Verify_transaction_snarks"
          , "Verifier_verify_transaction_snarks_done" )
    | "Verifier_verify_blockchain_snarks" ->
        Some
          ("Verify_blockchain_snarks", "Verifier_verify_blockchain_snarks_done")
    | "Verifier_verify_commands" ->
        Some ("Verify_commands", "Verifier_verify_commands_done")
    | "Prover_extend_blockchain" ->
        Some ("Produce_state_transition_proof", "Prover_extend_blockchain_done")
    | _ ->
        None
end

let push_pending_entry table call_id (entry : Pending.t) =
  Int.Table.update table call_id ~f:(fun entries ->
      let entries = Option.value ~default:[] entries in
      entry :: entries )

let find_nearest_block ~context_blocks timestamp =
  List.find_map context_blocks ~f:(fun (block, switch_timestamp) ->
      if Float.(timestamp >= switch_timestamp) then Some block else None )

let push_kimchi_checkpoints_from_metadata ~block_id parent_entry
    (metadata : Yojson.Safe.t) =
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
    let current_entry = ref parent_entry in
    List.iter checkpoints ~f:(function
      | `Checkpoint (checkpoint, timestamp) ->
          let checkpoint = "Kimchi_" ^ checkpoint in
          let prev_checkpoint = !current_entry.Block_trace.Entry.checkpoint in
          current_entry :=
            Block_tracing.record ~block_id ~checkpoint ~timestamp
              ~order:(`Chronological_after prev_checkpoint) ()
      | `Metadata metadata ->
          !current_entry.metadata <- `Assoc metadata )
  with exn ->
    eprintf "[WARN] failed to integrate kimchi checkpoints: %s\n%!"
      (Exn.to_string exn) ;
    eprintf "BACKTRACE:\n%s\n%!" (Printexc.get_backtrace ())

let add_pending_entries_to_block_trace ~block_id ~parent_checkpoint
    pending_entries =
  (* these must be processed at the end *)
  let pending_kimchi_entries = ref [] in
  let rec loop ~previous_checkpoint entries current_entry =
    match entries with
    | Pending.Checkpoint (checkpoint, timestamp) :: entries ->
        let current_entry =
          Block_tracing.record ~block_id ~checkpoint ~timestamp
            ~order:(`Chronological_after previous_checkpoint) ()
        in
        loop ~previous_checkpoint:checkpoint entries current_entry
    | Pending.Control ("metadata", data) :: entries ->
        if
          String.equal "Backend_tick_proof_create_async"
            current_entry.checkpoint
          || String.equal "Backend_tock_proof_create_async"
               current_entry.checkpoint
        then
          pending_kimchi_entries :=
            (current_entry, data) :: !pending_kimchi_entries
        else
          current_entry.metadata <-
            Yojson.Safe.Util.combine current_entry.metadata data ;
        loop ~previous_checkpoint entries current_entry
    | Pending.Control (_, _) :: entries ->
        (* printf "Ignoring control %s\n%!" other ; *)
        loop ~previous_checkpoint entries current_entry
    | [] ->
        List.iter !pending_kimchi_entries ~f:(fun (parent_entry, data) ->
            push_kimchi_checkpoints_from_metadata ~block_id parent_entry data )
  in
  loop ~previous_checkpoint:parent_checkpoint pending_entries
    (Block_trace.Entry.make ~timestamp:0.0 "")

(* Pending, complete verifier/prover traces are matched to a block trace by their timestamp
   and expected parent call (a checkpoint in the main trace). Once added they are removed
   from the pending table. *)
let process_pending_entries ~context_blocks
    (pending_checkpoints : Pending.registry) =
  Int.Table.filter_inplace pending_checkpoints ~f:(fun rev_pending_entries ->
      let pending_entries = List.rev rev_pending_entries in
      match pending_entries with
      | Checkpoint (first_checkpoint, first_timestamp) :: _ -> (
          match
            ( Pending.parent_and_finish_checkpoints first_checkpoint
            , List.hd rev_pending_entries )
          with
          | ( Some (parent_checkpoint, end_checkpoint)
            , Some (Checkpoint (last_checkpoint, _)) )
            when String.equal end_checkpoint last_checkpoint -> (
              match String.Table.find context_blocks parent_checkpoint with
              | Some context_blocks -> (
                  match find_nearest_block ~context_blocks first_timestamp with
                  | Some block_id ->
                      add_pending_entries_to_block_trace ~block_id
                        ~parent_checkpoint pending_entries ;
                      false
                  | None ->
                      true )
              | None ->
                  true )
          | _ ->
              true )
      | _ ->
          true (* TODO: warning, this shouldn't happen *) )

module Main_handler = struct
  let handle_verifier_and_prover_call_checkpoints checkpoint timestamp =
    match checkpoint with
    | "Verify_transaction_snarks"
    | "Verify_blockchain_snarks"
    | "Verify_commands" ->
        String.Table.update verifier_calls_context_block checkpoint
          ~f:(fun entries ->
            let entries = Option.value ~default:[] entries in
            (!current_block, timestamp) :: entries )
    | "Produce_state_transition_proof" ->
        String.Table.update prover_calls_context_block checkpoint
          ~f:(fun entries ->
            let entries = Option.value ~default:[] entries in
            (!current_block, timestamp) :: entries )
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
        (* TODO: should clear all old data here? *)
        ()
    | another ->
        eprintf "[WARN] unprocessed control: %s\n%!" another

  (* TODO: reset all traces too? *)
  let file_changed () =
    current_block := "0" ;
    String.Table.clear verifier_calls_context_block ;
    String.Table.clear prover_calls_context_block

  let main_trace_synced = Ivar.create ()

  let synced () = Ivar.read main_trace_synced

  let eof_reached () =
    process_pending_entries ~context_blocks:prover_calls_context_block
      Pending.prover_entries ;
    process_pending_entries ~context_blocks:verifier_calls_context_block
      Pending.verifier_entries ;
    Ivar.fill_if_empty main_trace_synced ()
end

module Prover_handler = struct
  let current_call_id = ref 0

  let process_checkpoint checkpoint timestamp =
    push_pending_entry Pending.prover_entries !current_call_id
      (Pending.Checkpoint (checkpoint, timestamp))

  let process_control tag data =
    if String.equal tag "current_call_id" then
      current_call_id := Yojson.Safe.Util.to_int data
    else
      push_pending_entry Pending.prover_entries !current_call_id
        (Pending.Control (tag, data))

  let file_changed () = Int.Table.clear Pending.prover_entries

  let eof_reached () = () (* Nothing to do *)
end

module Verifier_handler = struct
  let current_call_id = ref 0

  let process_checkpoint checkpoint timestamp =
    push_pending_entry Pending.verifier_entries !current_call_id
      (Pending.Checkpoint (checkpoint, timestamp))

  let process_control tag data =
    if String.equal tag "current_call_id" then
      current_call_id := Yojson.Safe.Util.to_int data
    else
      push_pending_entry Pending.verifier_entries !current_call_id
        (Pending.Control (tag, data))

  let file_changed () = Int.Table.clear Pending.verifier_entries

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
       let%bind () =
         Main_trace_processor.process_roated_files main_trace_file_path
       in
       let%bind () = Main_trace_processor.process_file main_trace_file_path
       and () =
         let%bind () = Main_handler.synced () in
         let%bind () =
           Prover_trace_processor.process_roated_files prover_trace_file_path
         in
         Prover_trace_processor.process_file prover_trace_file_path
       and () =
         let%bind () = Main_handler.synced () in
         let%bind () =
           Verifier_trace_processor.process_roated_files
             verifier_trace_file_path
         in
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
