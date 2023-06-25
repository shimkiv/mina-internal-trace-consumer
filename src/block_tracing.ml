open Core
module Checkpoint = Block_checkpoint
module Trace = Block_trace
module Structured_trace = Block_structured_trace

module Distributions = struct
  module D = Distribution.Make (struct
    type identity = Checkpoint.t [@@deriving yojson]
  end)

  include D

  type store = (Checkpoint.t, t) Hashtbl.t

  (* TODO: values can be stored as list, because key is value.identity *)
  let store_to_yojson s =
    let alist = Hashtbl.to_alist s in
    [%to_yojson: (Checkpoint.t * t) list] alist

  let store_of_yojson json =
    Result.map
      ([%of_yojson: (Checkpoint.t * t) list] json)
      ~f:(Hashtbl.of_alist_exn (module Checkpoint))

  let rec integrate_entry ~store entry =
    let { Structured_trace.Entry.checkpoint; duration; _ } = entry in
    record ~store checkpoint duration ;
    List.iter entry.checkpoints ~f:(integrate_entry ~store) ;
    ()
end

module Block_id = struct
  type t = string [@@deriving sexp, hash, compare]
end

module Registry = struct
  type t = (Block_id.t, Trace.t) Hashtbl.t

  type trace_info =
    { source : Trace.block_source
    ; blockchain_length : int
    ; global_slot : int
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

  let trace_info_to_yojson t =
    match trace_info_to_yojson t with
    | `Assoc fields ->
        let fields = filter_metadata_field fields in
        `Assoc fields
    | other ->
        other

  type traces = { traces : trace_info list; produced_traces : trace_info list }
  [@@deriving to_yojson]
end

let compute_source : Checkpoint.t -> Trace.block_source = function
  | "External_block_received" ->
      `External
  | "Begin_block_production" ->
      `Internal
  | "To_download" ->
      `Catchup
  | "Loaded_transition_from_storage" ->
      `Reconstruct
  | _ ->
      `Unknown

let compute_status : Checkpoint.t -> Trace.status = function
  | "Breadcrumb_integrated" ->
      `Success
  | "Failure" ->
      `Failure
  | _ ->
      `Pending
