open Core

type t =
  { distribution : Block_tracing.Distributions.t; mutable children : t list }
[@@deriving to_yojson]

type listing = t list [@@deriving to_yojson]

let of_flat_to_alist_item distribution =
  ( distribution.Block_tracing.Distributions.identity
  , { distribution; children = [] } )

let of_flat_distributions distributions =
  let results_table =
    Hashtbl.of_alist_exn
      (module Block_checkpoint)
      (List.map ~f:of_flat_to_alist_item distributions)
  in
  let is_children = ref String.Set.empty in
  List.iter distributions ~f:(fun d ->
      let children = Block_structured_trace.checkpoint_children d.identity in
      let entry = Hashtbl.find_exn results_table d.identity in
      List.iter children ~f:(fun child_id ->
          is_children := String.Set.add !is_children child_id ;
          match Hashtbl.find results_table child_id with
          | None ->
              (*Async.Log.Global.error
                "[WARN] expected to find child=%s of parent=%s but it was not \
                 there"
                child_id d.identity*)
              ()
          | Some child_entry ->
              entry.children <- child_entry :: entry.children ) ) ;
  Hashtbl.filter_inplace results_table ~f:(fun d ->
      not @@ String.Set.mem !is_children d.distribution.identity ) ;
  Hashtbl.to_alist results_table |> List.map ~f:snd
