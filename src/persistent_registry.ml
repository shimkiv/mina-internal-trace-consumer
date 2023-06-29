open Core
open Async

let block_id_trace_id_mapping = String.Table.create ()

let block_trace_status = Int.Table.create ()

let block_trace_id block_id =
  String.Table.find block_id_trace_id_mapping block_id

module Db = struct
  let set, get =
    let dbpool :
        ((module Caqti_async.CONNECTION), Caqti_error.t) Caqti_async.Pool.t
        option
        ref =
      ref None
    in
    let set_db pool = dbpool := Some pool in
    let get_db () =
      Option.value_exn ~message:"Database not initialized" !dbpool
    in
    (set_db, get_db)
end

let add_block_trace ~source block_id =
  let open Deferred.Result.Let_syntax in
  let db = Db.get () in
  let trace =
    Store.Persisted_block_trace.from_block_trace @@ Block_trace.empty source
  in
  let%map block_trace_id = Store.add_block_trace db block_id trace in
  ignore
  @@ String.Table.add ~key:block_id ~data:block_trace_id
       block_id_trace_id_mapping ;
  ignore
  @@ Int.Table.add ~key:block_trace_id ~data:trace.status block_trace_status ;
  block_trace_id

let get_or_add_block_trace ~source block_id =
  match String.Table.find block_id_trace_id_mapping block_id with
  | None ->
      add_block_trace ~source block_id
  | Some id ->
      return (Ok id)

let push_checkpoint block_trace_id ~is_main ~source ?(call_id = 0) ~name
    ~timestamp () =
  let pool = Db.get () in
  Store.add_block_trace_checkpoint pool block_trace_id is_main source call_id
    (`Checkpoint (name, timestamp))

let push_control block_trace_id ~is_main ~source ?(call_id = 0) ~name ~metadata
    () =
  let pool = Db.get () in
  Store.add_block_trace_checkpoint pool block_trace_id is_main source call_id
    (`Control (name, metadata))

let push_block_metadata block_trace_id ~metadata =
  let open Deferred.Result.Let_syntax in
  let pool = Db.get () in
  let%bind _, trace = Store.get_block_trace_by_id pool block_trace_id in
  let blockchain_length =
    Option.value ~default:trace.blockchain_length
      (Block_trace.extract_blockchain_length metadata)
  in
  let global_slot =
    Option.value ~default:trace.global_slot
      (Block_trace.extract_global_slot metadata)
  in
  let trace =
    { trace with
      blockchain_length
    ; global_slot
    ; metadata = Yojson.Safe.Util.combine trace.metadata (`Assoc metadata)
    }
  in
  Store.update_block_trace pool block_trace_id trace

let set_produced_block_state_hash block_trace_id state_hash =
  let pool = Db.get () in
  ignore
  @@ String.Table.add block_id_trace_id_mapping ~key:state_hash
       ~data:block_trace_id ;
  Store.update_block_trace_block_id pool block_trace_id state_hash

let get_distributions () =
  let open Deferred.Result.Let_syntax in
  let pool = Db.get () in
  let%map distributions = Store.get_value pool "checkpoint_distributions" in
  match distributions with
  | None ->
      Hashtbl.create (module Block_checkpoint)
  | Some json ->
      let ys = Yojson.Safe.from_string json in
      Result.ok_or_failwith ([%of_yojson: Block_tracing.Distributions.store] ys)

let save_distributions distributions =
  let pool = Db.get () in
  Store.set_value pool "checkpoint_distributions"
    ( Yojson.Safe.to_string
    @@ [%to_yojson: Block_tracing.Distributions.store] distributions )

let update_distributions (trace : Block_structured_trace.t) =
  let open Deferred.Result.Let_syntax in
  let%bind distributions = get_distributions () in
  List.iter trace.sections ~f:(fun section ->
      List.iter section.checkpoints
        ~f:(Block_tracing.Distributions.integrate_entry ~store:distributions) ) ;
  save_distributions distributions

let handle_status_change block_trace_id status =
  let open Deferred.Result.Let_syntax in
  match (status, Int.Table.find block_trace_status block_trace_id) with
  | _, Some `Success ->
      return ()
  | status, Some old_status
    when not @@ Block_trace.equal_status old_status status ->
      let pool = Db.get () in
      let%bind _block_id, trace =
        Store.get_block_trace_by_id pool block_trace_id
      in
      let%bind checkpoints =
        Store.get_block_trace_checkpoints pool ~main_trace:true block_trace_id
      in
      let timestamps =
        checkpoints
        |> List.filter_map ~f:(function
             | { source = `Main; checkpoint = `Checkpoint (_, t); call_id = _ }
               ->
                 Some t
             | _ ->
                 None )
      in
      let first_checkpoint =
        List.find_map_exn
          ~f:(function
            | { checkpoint = `Checkpoint (name, _); _ } -> Some name | _ -> None
            )
          checkpoints
      in
      let source = Block_tracing.compute_source first_checkpoint in
      let started_at =
        Option.value_exn @@ List.min_elt ~compare:Float.compare timestamps
      in
      let completed_at =
        Option.value_exn @@ List.max_elt ~compare:Float.compare timestamps
      in
      let total_time = completed_at -. started_at in
      let trace = { trace with status; started_at; total_time; source } in
      let%bind () = Store.update_block_trace pool block_trace_id trace in
      ignore
      @@ Int.Table.add block_trace_status ~key:block_trace_id ~data:status ;
      let%bind () =
        if Block_trace.equal_status status `Success then
          let trace =
            Store.Persisted_block_trace.to_block_trace ~checkpoints trace
          in
          let trace = Block_structured_trace.of_flat_trace trace in
          update_distributions trace
        else return ()
      in
      return ()
  | _ ->
      return ()

let get_block_trace_checkpoints block_trace_id =
  let pool = Db.get () in
  Store.get_block_trace_checkpoints pool ~main_trace:true block_trace_id

let get_block_trace_other_checkpoints block_trace_id =
  let pool = Db.get () in
  Store.get_block_trace_checkpoints pool ~main_trace:false block_trace_id

let get_block_traces block_id =
  let pool = Db.get () in
  Store.get_block_traces pool block_id

let get_all_block_traces ?max_length ?offset ?height ?global_slot ?chain_length
    ?order () =
  let pool = Db.get () in
  Store.get_block_trace_info_entries pool ?max_length ?offset ?height
    ?global_slot ?chain_length ?order ()

let only_main_checkpoints =
  List.filter_map ~f:(function
    | { Store.source = `Main
      ; checkpoint = `Checkpoint (name, timestamp)
      ; call_id = _
      } ->
        Some (name, timestamp)
    | _ ->
        None )

let nearest_trace ~prev_checkpoint ~timestamp block_trace_id =
  let open Deferred.Result.Let_syntax in
  let%bind checkpoints = get_block_trace_checkpoints block_trace_id in
  let checkpoints = only_main_checkpoints checkpoints in
  let%bind other_checkpoints =
    get_block_trace_other_checkpoints block_trace_id
  in
  let other_checkpoints = only_main_checkpoints other_checkpoints in
  let left =
    List.find checkpoints ~f:(fun (checkpoint, started_at) ->
        Float.(started_at <= timestamp)
        && String.equal checkpoint prev_checkpoint )
  in
  let right =
    List.find other_checkpoints ~f:(fun (checkpoint, started_at) ->
        Float.(started_at <= timestamp)
        && String.equal checkpoint prev_checkpoint )
  in
  return
  @@
  match (left, right) with
  | None, None ->
      `Main
  | Some _, None ->
      `Main
  | None, Some _ ->
      `Other
  | Some (_, lstarted_at), Some (_, rstarted_at) ->
      if Float.(lstarted_at > rstarted_at) then `Main else `Other
