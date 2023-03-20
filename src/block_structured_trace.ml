open Core
module Checkpoint = Block_checkpoint
module Trace = Block_trace

module Entry = struct
  type t =
    { checkpoint : Checkpoint.t
    ; started_at : float
    ; duration : float
    ; metadata : Yojson.Safe.t
    ; checkpoints : t list
    }
  [@@deriving to_yojson]

  let of_flat_entry entry =
    let { Trace.Entry.checkpoint; started_at; duration; metadata } = entry in
    { checkpoint; started_at; duration; metadata; checkpoints = [] }
end

type section = { title : string; checkpoints : Entry.t list }
[@@deriving to_yojson]

type t =
  { source : Trace.block_source
  ; blockchain_length : int [@key "blockchain_length_int"]
  ; sections : section list
  ; status : Trace.status
  ; total_time : float
  ; metadata : Yojson.Safe.t
  }
[@@deriving to_yojson]

let to_yojson t =
  let blockchain_length_string = Int.to_string t.blockchain_length in
  match to_yojson t with
  | `Assoc fields ->
      `Assoc (("blockchain_length", `String blockchain_length_string) :: fields)
  | other ->
      other

let checkpoint_children (c : Checkpoint.t) : Checkpoint.t list =
  match c with
  | "Initial_validation" ->
      [ "Verify_blockchain_snarks"; "Verify_blockchain_snarks_done" ]
  | "Validate_transition" ->
      [ "Check_transition_not_in_frontier"
      ; "Check_transition_not_in_process"
      ; "Check_transition_can_be_connected"
      ; "Register_transition_for_processing"
      ]
  | "Build_breadcrumb" ->
      [ "Validate_staged_ledger_diff"; "Create_breadcrumb" ]
  | "Validate_staged_ledger_diff" ->
      [ "Check_completed_works"; "Prediff"; "Apply_diff"; "Diff_applied" ]
  | "Prediff" ->
      [ "Verify_commands"; "Verify_commands_done" ]
  | "Check_completed_works" ->
      [ "Verify_transaction_snarks"; "Verify_transaction_snarks_done" ]
  | "Apply_diff" ->
      [ "Update_coinbase_stack"
      ; "Update_coinbase_stack_done"
      ; "Check_for_sufficient_snark_work"
      ; "Check_zero_fee_excess"
      ; "Fill_work_and_enqueue_transactions"
      ; "Update_pending_coinbase_collection"
      ; "Verify_scan_state_after_apply"
      ; "Hash_new_staged_ledger"
      ; "Hash_new_staged_ledger_done"
      ; "Make_staged_ledger_hash"
      ]
  | "Update_coinbase_stack" ->
      [ "Update_ledger_and_get_statements"
      ; "Update_ledger_and_get_statements_done"
      ]
  | "Hash_new_staged_ledger" ->
      [ "Hash_scan_state"; "Get_merkle_root" ]
  | "Add_and_finalize" ->
      [ "Add_breadcrumb_to_frontier"; "Add_breadcrumb_to_frontier_done" ]
  | "Add_breadcrumb_to_frontier" ->
      [ "Calculate_diffs"
      ; "Calculate_diffs_done"
      ; "Apply_catchup_tree_diffs"
      ; "Apply_full_frontier_diffs"
      ; "Apply_full_frontier_diffs_done"
      ; "Synchronize_persistent_frontier"
      ; "Synchronize_persistent_frontier_done"
      ; "Notify_frontier_extensions"
      ; "Notify_frontier_extensions_done"
      ]
  | "Apply_full_frontier_diffs" ->
      [ "Move_frontier_root"; "Move_frontier_root_done" ]
  | "Notify_frontier_extensions" ->
      [ "Update_frontier_extension"
      ; "Update_frontier_extension_done"
      ; "Notify_SPRC_handle_diffs"
      ; "Notify_SPRC_write_view"
      ; "Notify_SPRC_write_view_done"
      ]
  | "Notify_SPRC_handle_diffs" ->
      [ "SPRC_add_scan_state_to_ref_table"
      ; "SPRC_add_scan_state_to_ref_table_done"
      ; "SPRC_add_to_work_table"
      ; "SPRC_add_to_work_table_done"
      ; "SPRC_remove_from_work_table"
      ; "SPRC_remove_from_work_table_done"
      ; "SPRC_update_best_tip_table"
      ; "SPRC_update_best_tip_table_done"
      ]
  | "Generate_next_state" ->
      [ "Create_staged_ledger_diff"
      ; "Create_staged_ledger_diff_done"
      ; "Apply_staged_ledger_diff"
      ; "Apply_staged_ledger_diff_done"
      ; "Generate_transition"
      ; "Generate_transition_done"
      ]
  | "Generate_transition" ->
      [ "Consensus_state_update"; "Consensus_state_update_done" ]
  | "Produce_state_transition_proof" ->
      [ "Prover_extend_blockchain"; "Prover_extend_blockchain_done" ]
  | "Prover_extend_blockchain" ->
      [ "Pickles_step_proof"
      ; "Pickles_wrap_proof"
      ; "Pickles_step_proof_done"
      ; "Pickles_wrap_proof_done"
      ]
  | "Pickles_step_proof" ->
      [ "Step_generate_witness_conv"
      ; "Step_compute_prev_proof_parts"
      ; "Step_compute_prev_proof_parts_done"
      ; "Step_compute_bulletproof_challenges"
      ; "Step_compute_bulletproof_challenges_done"
      ; "Backend_tick_proof_create_async"
      ; "Backend_tick_proof_create_async_done"
      ]
  | "Pickles_wrap_proof" ->
      [ "Wrap_compute_deferred_values"
      ; "Wrap_compute_deferred_values_done"
      ; "Wrap_generate_witness_conv"
      ; "Wrap_verifier_incrementally_verify_proof"
      ; "Wrap_verifier_incrementally_verify_proof_done"
      ; "Backend_tock_proof_create_async"
      ; "Backend_tock_proof_create_async_done"
      ]
  | "Backend_tick_proof_create_async" | "Backend_tock_proof_create_async" ->
      [ "Kimchi_pasta_fp_plonk_proof_create"
      ; "Kimchi_pasta_fq_plonk_proof_create"
      ]
  | "Kimchi_pasta_fp_plonk_proof_create" | "Kimchi_pasta_fq_plonk_proof_create"
    ->
      [ "Kimchi_create_recursive"; "Kimchi_create_recursive_done" ]
  | "Kimchi_create_recursive" ->
      [ "Kimchi_pad_witness"
      ; "Kimchi_set_up_fq_sponge"
      ; "Kimchi_commit_to_witness_columns"
      ; "Kimchi_z_permutation_aggregation_polynomial"
      ; "Kimchi_eval_witness_polynomials_over_domains"
      ; "Kimchi_compute_index_evals"
      ; "Kimchi_compute_quotient_poly"
      ; "Kimchi_lagrange_basis_eval_zeta_poly"
      ; "Kimchi_lagrange_basis_eval_zeta_omega_poly"
      ; "Kimchi_chunk_eval_zeta_omega_poly"
      ; "Kimchi_compute_ft_poly"
      ; "Kimchi_ft_eval_zeta_omega"
      ; "Kimchi_build_polynomials"
      ; "Kimchi_create_aggregated_evaluation_proof"
      ]
  | "Apply_staged_ledger_diff" ->
      [ "Update_coinbase_stack"
      ; "Update_coinbase_stack_done"
      ; "Check_for_sufficient_snark_work"
      ; "Check_zero_fee_excess"
      ; "Fill_work_and_enqueue_transactions"
      ; "Update_pending_coinbase_collection"
      ; "Verify_scan_state_after_apply"
      ; "Hash_new_staged_ledger"
      ; "Hash_new_staged_ledger_done"
      ; "Make_staged_ledger_hash"
      ]
  | "Create_staged_ledger_diff" ->
      [ "Get_snark_work_for_pending_transactions"
      ; "Validate_and_apply_transactions"
      ; "Generate_staged_ledger_diff"
      ; "Generate_staged_ledger_diff_done"
      ]
  | "Produce_validated_transition" ->
      [ "Build_breadcrumb" ]
  | "Wait_for_confirmation" ->
      [ "Begin_local_block_processing"
      ; "Add_and_finalize"
      ; "Add_and_finalize_done"
      ]
  | "Validate_proofs" ->
      [ "Verifier_verify_blockchain_snarks"
      ; "Verifier_verify_blockchain_snarks_done"
      ]
  | "Verify_blockchain_snarks" ->
      [ "Verifier_verify_blockchain_snarks"
      ; "Verifier_verify_blockchain_snarks_done"
      ]
  | "Verify_transaction_snarks" ->
      [ "Verifier_verify_transaction_snarks"
      ; "Verifier_verify_transaction_snarks_done"
      ]
  | "Verify_commands" ->
      [ "Verifier_verify_commands"; "Verifier_verify_commands_done" ]
  | "Verifier_verify_blockchain_snarks" ->
      [ "Wrap_verifier_incrementally_verify_proof"
      ; "Wrap_verifier_incrementally_verify_proof_done"
      ; "Verify_heterogenous"
      ]
  | "Verifier_verify_transaction_snarks" ->
      [ "Verify_heterogenous" ]
  | "Verifier_verify_commands" ->
      [ "Verify_heterogenous" ]
  | "Verify_heterogenous" ->
      [ "Compute_plonks_and_chals"
      ; "Compute_plonks_and_chals_done"
      ; "Accumulator_check"
      ; "Accumulator_check_done"
      ; "Compute_batch_verify_inputs"
      ; "Compute_batch_verify_inputs_done"
      ; "Dlog_check_batch_verify"
      ; "Dlog_check_batch_verify_done"
      ]
  | "Dlog_check_batch_verify" ->
      [ "Batch_verify_backend_convert_inputs"
      ; "Batch_verify_backend_convert_inputs_done"
      ; "Batch_verify_backend"
      ; "Batch_verify_backend_done"
      ]
  | _ ->
      []

let is_parent_of parent entry =
  let children = checkpoint_children parent.Entry.checkpoint in
  List.mem children entry.Entry.checkpoint ~equal:Checkpoint.equal

let has_children entry =
  not (List.is_empty (checkpoint_children entry.Entry.checkpoint))

(* TODOX: instead of processing checkpoints at the same level here
   process the flat trace using information about parent/children relationship
   to skip children when finding the end *)
let postprocess_checkpoints checkpoints =
  match checkpoints with
  | [] ->
      []
  | first :: _ as checkpoints ->
      let next_timestamp = ref first.Entry.started_at in
      List.rev_map checkpoints ~f:(fun entry ->
          let ended_at = !next_timestamp in
          next_timestamp := entry.started_at ;
          { entry with duration = ended_at -. entry.started_at } )

let postprocess_entry_checkpoints entry =
  let checkpoints = postprocess_checkpoints entry.Entry.checkpoints in
  { entry with checkpoints }

let merge_into_parent parent entry =
  let entry' = postprocess_entry_checkpoints entry in
  let checkpoints = entry' :: parent.Entry.checkpoints in
  { parent with checkpoints }

let rec collapse_pending_stack_with_children entry acc stack =
  match stack with
  | parent :: _ as stack when is_parent_of parent entry ->
      (acc, entry :: stack)
  | child :: parent :: rest ->
      let parent' = merge_into_parent parent child in
      collapse_pending_stack_with_children entry acc (parent' :: rest)
  | [ sibling ] ->
      let sibling' = postprocess_entry_checkpoints sibling in
      (sibling' :: acc, [ entry ])
  | [] ->
      (acc, [ entry ])

let rec collapse_pending_stack_simple entry acc stack =
  match stack with
  | parent :: rest when is_parent_of parent entry ->
      let parent' = merge_into_parent parent entry in
      (acc, parent' :: rest)
  | child :: parent :: rest ->
      let parent' = merge_into_parent parent child in
      collapse_pending_stack_simple entry acc (parent' :: rest)
  | [ sibling ] ->
      let sibling' = postprocess_entry_checkpoints sibling in
      (entry :: sibling' :: acc, [])
  | [] ->
      (entry :: acc, [])

let structure_checkpoints checkpoints =
  let checkpoints = List.rev_map ~f:Entry.of_flat_entry checkpoints in
  postprocess_checkpoints @@ fst
  @@ List.fold checkpoints ~init:([], []) ~f:(fun (accum, stack) entry ->
         if has_children entry then
           collapse_pending_stack_with_children entry accum stack
         else collapse_pending_stack_simple entry accum stack )

(* For produced traces, we want "Transition_accepted" moved later to get a better structure *)
let adjust_transition_accepted_checkpoint checkpoints_rev =
  let after, before =
    List.split_while checkpoints_rev ~f:(fun entry ->
        not @@ String.equal entry.Trace.Entry.checkpoint "Transition_accepted" )
  in
  let transition_accepted = List.hd_exn before in
  let before = List.tl_exn before in
  let last = List.hd_exn after in
  let after = List.tl_exn after in
  [ last; transition_accepted ] @ after @ before

let adjust_transition_accepted_checkpoint (checkpoints_rev : Trace.Entry.t list)
    =
  match (List.hd checkpoints_rev, List.last checkpoints_rev) with
  | Some last, Some first
    when String.equal first.checkpoint "Begin_block_production"
         && String.equal last.checkpoint "Breadcrumb_integrated" ->
      adjust_transition_accepted_checkpoint checkpoints_rev
  | _ ->
      checkpoints_rev

let of_flat_trace trace =
  let { Trace.source
      ; blockchain_length
      ; status
      ; checkpoints
      ; other_checkpoints = _
      ; total_time
      ; metadata
      } =
    trace
  in
  let checkpoints = adjust_transition_accepted_checkpoint checkpoints in
  let checkpoints = structure_checkpoints checkpoints in
  (* TODO: complete sections or remove (probably remove, not useful anymore) *)
  let section = { title = "All"; checkpoints } in
  let sections = [ section ] in
  { source; blockchain_length; sections; status; total_time; metadata }
