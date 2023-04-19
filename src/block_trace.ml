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

type insert_order = [ `Append | `Chronological_after of string ]

type block_source =
  [ `External | `Internal | `Catchup | `Reconstruct | `Unknown ]
[@@deriving to_yojson, equal]

type status = [ `Pending | `Failure | `Success ] [@@deriving to_yojson, equal]

let block_source_to_yojson = Util.flatten_yojson_variant block_source_to_yojson

let status_to_yojson = Util.flatten_yojson_variant status_to_yojson

type t =
  { source : block_source
  ; blockchain_length : int
  ; global_slot : int
  ; checkpoints : Entry.t list
  ; other_checkpoints : Entry.t list
  ; status : status
  ; mutable total_time : float
  ; metadata : Yojson.Safe.t
  }
[@@deriving to_yojson]

let empty ?(blockchain_length = 0) source =
  { source
  ; blockchain_length
  ; global_slot = 0
  ; checkpoints = []
  ; other_checkpoints = []
  ; status = `Pending
  ; total_time = 0.0
  ; metadata = `Assoc []
  }

let to_yojson t = to_yojson { t with checkpoints = List.rev t.checkpoints }

let started_at t =
  if equal_status `Success t.status then
    List.hd t.checkpoints
    |> Option.value_map ~default:(-1.0) ~f:(fun cp ->
           cp.started_at -. t.total_time )
  else
    List.last t.checkpoints
    |> Option.value_map ~default:(-1.0) ~f:(fun cp -> cp.started_at)

let recalculate_total trace =
  try
    let started_at = (List.last_exn trace.checkpoints).started_at in
    let finished_at = (List.hd_exn trace.checkpoints).started_at in
    trace.total_time <- finished_at -. started_at
  with _ ->
    eprintf "[WARN] failure when trying to recalculate trace total time\n%!"

let push_metadata ~metadata trace =
  match trace with
  | None | Some { checkpoints = []; _ } ->
      trace
  | Some
      ( { checkpoints = previous :: _
        ; other_checkpoints = other_previous :: other_rest
        ; _
        } as trace )
    when Float.(other_previous.started_at > previous.started_at) ->
      (* If the last checkpoint is in other traces we have to attach metadata there *)
      let other_previous =
        { other_previous with
          metadata =
            Yojson.Safe.Util.combine other_previous.metadata (`Assoc metadata)
        }
      in
      Some { trace with other_checkpoints = other_previous :: other_rest }
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

let extract_global_slot metadata =
  List.Assoc.find metadata ~equal:String.equal "global_slot"
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
      let global_slot =
        Option.value ~default:trace.global_slot (extract_global_slot metadata)
      in
      Some
        { trace with
          blockchain_length
        ; global_slot
        ; metadata = Yojson.Safe.Util.combine trace.metadata (`Assoc metadata)
        }

let readjust_from_previous orig_after orig_before prev_checkpoint =
  let after, before =
    List.split_while orig_before ~f:(fun previous_entry ->
        not (String.equal previous_entry.Entry.checkpoint prev_checkpoint) )
  in
  (orig_after @ after, before)

let push ~status ~source ~order ?blockchain_length entry trace =
  match (trace, order) with
  | None, _ ->
      let trace = empty ?blockchain_length source in
      { trace with checkpoints = [ entry ]; status }
  | Some ({ checkpoints = []; _ } as trace), _ ->
      { trace with checkpoints = [ entry ]; status }
  | Some ({ checkpoints; _ } as trace), `Chronological_after prev_checkpoint
    -> (
      let after, before =
        List.split_while checkpoints ~f:(fun previous_entry ->
            Float.(previous_entry.started_at >= entry.started_at) )
      in
      let after, before = readjust_from_previous after before prev_checkpoint in
      try
        let previous, before = (List.hd_exn before, List.tl_exn before) in
        if not (String.equal previous.checkpoint prev_checkpoint) then
          eprintf "[ERROR] expected previous checkpoint %s but got %s\n%!"
            prev_checkpoint previous.checkpoint ;
        let previous =
          { previous with duration = entry.started_at -. previous.started_at }
        in
        (* FIXME: set duration for the new added entry *)
        { trace with checkpoints = after @ (entry :: previous :: before) }
      with _ ->
        (*printf
          "[ERROR] when trying to add checkpoint %s after %s @ %f for block \
           length %d\n\
           %!"
          entry.checkpoint prev_checkpoint entry.started_at
          trace.blockchain_length ;*)
        trace )
  | Some ({ checkpoints = previous :: rest; _ } as trace), _
    when not (equal_status trace.status `Success)
         (* || not (equal_block_source source `External) *) ->
      (* Only add checkpoints to the main list if processing has not been completed before *)
      let previous =
        { previous with duration = entry.started_at -. previous.started_at }
      in
      let total_time = trace.total_time +. previous.duration in
      { trace with checkpoints = entry :: previous :: rest; status; total_time }
  | Some ({ other_checkpoints = []; _ } as trace), _ ->
      { trace with other_checkpoints = [ entry ] }
  | Some ({ other_checkpoints = previous :: rest; _ } as trace), _ ->
      let previous =
        { previous with duration = entry.started_at -. previous.started_at }
      in
      { trace with other_checkpoints = entry :: previous :: rest }
