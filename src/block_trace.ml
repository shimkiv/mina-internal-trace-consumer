open Core
module Checkpoint = Block_checkpoint

module Entry = struct
  type t =
    { checkpoint : Checkpoint.t
    ; started_at : float
    ; duration : float
    ; mutable metadata : Yojson.Safe.t
    }
  [@@deriving to_yojson]

  let make ?(metadata = []) ~timestamp checkpoint =
    let started_at = timestamp in
    (* Duration will be adjusted during post-processing *)
    let duration = 0.0 in
    { checkpoint; started_at; duration; metadata = `Assoc metadata }
end

type block_source =
  [ `External | `Internal | `Catchup | `Reconstruct | `Unknown ]
[@@deriving to_yojson, equal]

type status = [ `Pending | `Failure | `Success ] [@@deriving to_yojson, equal]

let block_source_to_yojson = Util.flatten_yojson_variant block_source_to_yojson

let status_to_yojson = Util.flatten_yojson_variant status_to_yojson

type t =
  { source : block_source
  ; blockchain_length : int
  ; checkpoints : Entry.t list
  ; other_checkpoints : Entry.t list
  ; status : status
  ; total_time : float
  ; metadata : Yojson.Safe.t
  }
[@@deriving to_yojson]

let empty ?(blockchain_length = 0) source =
  { source
  ; blockchain_length
  ; checkpoints = []
  ; other_checkpoints = []
  ; status = `Pending
  ; total_time = 0.0
  ; metadata = `Assoc []
  }

let to_yojson t = to_yojson { t with checkpoints = List.rev t.checkpoints }

let started_at t =
  List.hd t.checkpoints
  |> Option.value_map ~default:(-1.0) ~f:(fun cp -> cp.started_at)

let push_metadata ~metadata trace =
  match trace with
  | None | Some { checkpoints = []; _ } ->
      trace (* do nothing *)
  | Some ({ checkpoints = previous :: rest; _ } as trace) ->
      let previous =
        { previous with
          metadata = Yojson.Safe.Util.combine previous.metadata (`Assoc metadata)
        }
      in
      Some { trace with checkpoints = previous :: rest }

let extract_blockchain_length metadata =
  List.Assoc.find metadata ~equal:String.equal "blockchain_length"
  |> Option.bind ~f:(fun json -> Yojson.Safe.Util.to_string_option json)
  |> Option.map ~f:Int.of_string

let push_global_metadata ~metadata trace =
  match trace with
  | None ->
      trace (* do nothing *)
  | Some trace ->
      let blockchain_length =
        Option.value ~default:trace.blockchain_length
          (extract_blockchain_length metadata)
      in
      Some
        { trace with
          blockchain_length
        ; metadata = Yojson.Safe.Util.combine trace.metadata (`Assoc metadata)
        }

let push ~status ~source ~ordered ?blockchain_length entry trace =
  match trace with
  | None ->
      let trace = empty ?blockchain_length source in
      { trace with checkpoints = [ entry ]; status }
  | Some ({ checkpoints = []; _ } as trace) ->
      { trace with checkpoints = [ entry ]; status }
  | Some ({ checkpoints; _ } as trace) when ordered ->
      let after, before =
        List.split_while checkpoints ~f:(fun previous_entry ->
            Float.(previous_entry.started_at >= entry.started_at) )
      in
      let previous, before = (List.hd_exn before, List.tl_exn before) in
      let previous =
        { previous with duration = entry.started_at -. previous.started_at }
      in
      (* FIXME: set duration for the new added entry *)
      { trace with checkpoints = after @ (entry :: previous :: before) }
  | Some ({ checkpoints = previous :: rest; _ } as trace)
    when equal_status trace.status `Pending
         || not (equal_block_source source `External) ->
      (* Only add checkpoints to the main list if processing has not been completed before *)
      let previous =
        { previous with duration = entry.started_at -. previous.started_at }
      in
      let total_time = trace.total_time +. previous.duration in
      { trace with checkpoints = entry :: previous :: rest; status; total_time }
  | Some ({ other_checkpoints = []; _ } as trace) ->
      { trace with other_checkpoints = [ entry ] }
  | Some ({ other_checkpoints = previous :: rest; _ } as trace) ->
      let previous =
        { previous with duration = entry.started_at -. previous.started_at }
      in
      { trace with other_checkpoints = entry :: previous :: rest }
