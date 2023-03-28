open Core
module Checkpoint = Block_checkpoint
module Trace = Block_trace
module Structured_trace = Block_structured_trace

module Distributions = struct
  module D = Distribution.Make (struct
    type identity = Checkpoint.t [@@deriving to_yojson]
  end)

  include D

  type store = (Checkpoint.t, t) Hashtbl.t

  let all_store : store = Hashtbl.create (module Checkpoint)

  let produced_store : store = Hashtbl.create (module Checkpoint)

  let external_store : store = Hashtbl.create (module Checkpoint)

  let catchup_store : store = Hashtbl.create (module Checkpoint)

  let reconstruct_store : store = Hashtbl.create (module Checkpoint)

  let unknown_store : store = Hashtbl.create (module Checkpoint)

  let source_store = function
    | `Catchup ->
        catchup_store
    | `Internal ->
        produced_store
    | `External ->
        external_store
    | `Reconstruct ->
        reconstruct_store
    | `Unknown ->
        unknown_store

  let rec integrate_entry ~store entry =
    let { Structured_trace.Entry.checkpoint; duration; _ } = entry in
    record ~store checkpoint duration ;
    List.iter entry.checkpoints ~f:(integrate_entry ~store) ;
    ()

  let integrate_trace (trace : Structured_trace.t) =
    let source_store = source_store trace.source in
    List.iter trace.sections ~f:(fun section ->
        List.iter section.checkpoints ~f:(integrate_entry ~store:all_store) ;
        List.iter section.checkpoints ~f:(integrate_entry ~store:source_store) )

  let all () = Hashtbl.data all_store
end

module Block_id = struct
  type t = string [@@deriving sexp, hash, compare]
end

module Registry = struct
  type t = (Block_id.t, Trace.t) Hashtbl.t

  type trace_info =
    { source : Trace.block_source
    ; blockchain_length : int
    ; state_hash : string
    ; status : Trace.status
    ; started_at : float
    ; total_time : float
    ; metadata : Yojson.Safe.t
    }
  [@@deriving to_yojson]

  let filter_metadata_field (fields : (string * Yojson.Safe.t) list) =
    match List.Assoc.find fields ~equal:String.equal "metadata" with
    | Some (`Assoc metadata) ->
        let fields = List.Assoc.remove fields ~equal:String.equal "metadata" in
        let metadata =
          `Assoc (List.Assoc.remove metadata ~equal:String.equal "diff_log")
        in
        List.Assoc.add fields ~equal:String.equal "metadata" metadata
    | _ ->
        fields

  type traces = { traces : trace_info list; produced_traces : trace_info list }
  [@@deriving to_yojson]

  let registry : t = Hashtbl.create (module Block_id)

  let postprocess_checkpoints trace =
    let next_timestamp = ref (List.hd_exn trace).Trace.Entry.started_at in
    List.map trace ~f:(fun entry ->
        let ended_at = !next_timestamp in
        next_timestamp := entry.started_at ;
        { entry with duration = ended_at -. entry.started_at } )

  let find_trace state_hash = Hashtbl.find registry state_hash

  let all_traces ?max_length ?height () =
    let matches_height blockchain_length =
      Option.value_map ~default:true ~f:(( = ) blockchain_length) height
    in
    let traces =
      Hashtbl.to_alist registry
      |> List.filter_map ~f:(fun (key, item) ->
             match key with
             | global_slot when String.length global_slot < 30 ->
                 None
             | state_hash ->
                 let Trace.
                       { blockchain_length
                       ; source
                       ; status
                       ; total_time
                       ; metadata
                       ; _
                       } =
                   item
                 in
                 if matches_height blockchain_length then
                   Some
                     { state_hash
                     ; blockchain_length
                     ; source
                     ; status
                     ; started_at = Trace.started_at item
                     ; total_time
                     ; metadata
                     }
                 else None )
    in
    let traces =
      traces
      |> List.sort ~compare:(fun a b ->
             Int.compare a.blockchain_length b.blockchain_length )
    in
    let produced_traces =
      Hashtbl.to_alist registry
      |> List.filter_map ~f:(fun (key, item) ->
             match key with
             | state_hash when String.length state_hash > 30 ->
                 None
             | _ ->
                 let state_hash = "<unknown>" in
                 let Trace.
                       { blockchain_length
                       ; source
                       ; status
                       ; total_time
                       ; metadata
                       ; _
                       } =
                   item
                 in
                 Some
                   { state_hash
                   ; blockchain_length
                   ; source
                   ; status
                   ; started_at = Trace.started_at item
                   ; total_time
                   ; metadata
                   } )
      |> List.sort ~compare:(fun a b ->
             Int.compare a.blockchain_length b.blockchain_length )
    in
    match max_length with
    | None ->
        { traces; produced_traces }
    | Some max_length ->
        let traces_count = List.length traces in
        let produced_traces_count = List.length produced_traces in
        let traces = List.drop traces (traces_count - max_length) in
        let produced_traces =
          List.drop produced_traces (produced_traces_count - max_length)
        in
        { traces; produced_traces }

  let push_entry ~status ~source ~order ?blockchain_length block_id entry =
    Hashtbl.update registry block_id
      ~f:(Trace.push ~status ~source ~order ?blockchain_length entry)

  let push_metadata ~metadata block_id =
    Hashtbl.change registry block_id ~f:(Trace.push_metadata ~metadata)

  let push_global_metadata ~metadata block_id =
    Hashtbl.change registry block_id ~f:(Trace.push_global_metadata ~metadata)

  let move_trace ~from ~into =
    match Hashtbl.find_and_remove registry from with
    | Some trace ->
        ignore @@ Hashtbl.add registry ~key:into ~data:trace
    | None ->
        ()
end

let external_source_checkpoints =
  [ "External_block_received"
  ; "Initial_validation"
  ; "Verify_blockchain_snarks"
  ; "Verify_blockchain_snarks_done"
  ; "Initial_validation_done"
  ; "Validate_transition"
  ; "Check_transition_not_in_frontier"
  ; "Check_transition_not_in_process"
  ; "Check_transition_can_be_connected"
  ; "Register_transition_for_processing"
  ; "Validate_transition_done"
  ; "Failure"
  ]

let _is_external_checkpoint =
  List.mem external_source_checkpoints ~equal:String.equal

let compute_source : Checkpoint.t -> Trace.block_source = function
  | "External_block_received" ->
      `External
  | "Begin_block_production" ->
      `Internal
  | "To_download" ->
      `Catchup
  | "Loaded_transition_from_storage" ->
      `Reconstruct
  (*| external_checkpoint when is_external_checkpoint external_checkpoint ->
      `External*)
  | _ ->
      `Unknown

let compute_status : Checkpoint.t -> Trace.status = function
  | "Breadcrumb_integrated" ->
      `Success
  | "Failure" ->
      `Failure
  | _ ->
      `Pending

let handle_status_change status block_id =
  match (status, Hashtbl.find Registry.registry block_id) with
  | `Success, Some trace
    when not @@ Trace.equal_status trace.Trace.status `Success ->
      Block_trace.recalculate_total trace ;
      let structured = Structured_trace.of_flat_trace trace in
      Distributions.integrate_trace structured
  | _ ->
      ()

let checkpoint ?status ?metadata ?blockchain_length ~block_id ?source
    ?(order = `Append) ~checkpoint ~timestamp () =
  let source =
    match source with
    | None ->
        compute_source checkpoint
    | Some source ->
        source
  in
  let status =
    match status with
    | Some status ->
        status
    | None ->
        compute_status checkpoint
  in
  handle_status_change status block_id ;
  let entry = Trace.Entry.make ?metadata ~timestamp checkpoint in
  Registry.push_entry ~status ~source ~order ?blockchain_length block_id entry ;
  entry

let failure ~reason =
  checkpoint
    ~metadata:[ ("reason", `String reason) ]
    ~status:`Failure ~checkpoint:"Failure"

let push_metadata ~block_id metadata = Registry.push_metadata ~metadata block_id

let push_global_metadata ~block_id metadata =
  Registry.push_global_metadata ~metadata block_id

let set_produced_block_state_hash ~block_id state_hash =
  Registry.move_trace ~from:block_id ~into:state_hash

let record = checkpoint

let record_failure = failure

let nearest_trace ~prev_checkpoint ~timestamp block_id =
  let trace = Option.value_exn @@ Registry.find_trace block_id in
  let { Trace.checkpoints; other_checkpoints; _ } = trace in
  let left =
    List.find checkpoints ~f:(fun cp ->
        Float.(cp.Trace.Entry.started_at <= timestamp)
        && String.equal cp.checkpoint prev_checkpoint )
  in
  let right =
    List.find other_checkpoints ~f:(fun cp ->
        Float.(cp.Trace.Entry.started_at <= timestamp)
        && String.equal cp.checkpoint prev_checkpoint )
  in
  match (left, right) with
  | None, None ->
      `Main
  | Some _, None ->
      `Main
  | None, Some _ ->
      `Other
  | Some lcp, Some rcp ->
      if Float.(lcp.started_at > rcp.started_at) then `Main else `Other
