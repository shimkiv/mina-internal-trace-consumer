open Core

module Make (Inputs : sig
  type identity [@@deriving to_yojson]
end) =
struct
  open Inputs

  type range_info =
    { mutable count : int
    ; mutable mean_time : float [@key "meanTime"]
    ; mutable max_time : float [@key "maxTime"]
    ; mutable total_time : float [@key "totalTime"]
    }
  [@@deriving to_yojson]

  type t =
    { identity : identity
    ; mutable count : int
    ; mutable total_time : float [@key "totalTime"]
    ; one_to_ten_us : range_info [@key "oneToTenUs"]
    ; ten_to_one_hundred_us : range_info [@key "tenToOneHundredUs"]
    ; one_hundred_us_to_one_ms : range_info [@key "oneHundredUsToOneMs"]
    ; one_to_ten_ms : range_info [@key "oneToTenMs"]
    ; ten_to_one_hundred_ms : range_info [@key "tenToOneHundredMs"]
    ; one_hundred_ms_to_one_s : range_info [@key "oneHundredMsToOneS"]
    ; one_to_ten_s : range_info [@key "oneToTenS"]
    ; ten_to_one_hundred_s : range_info [@key "tenToOneHundredS"]
    ; one_hundred_s : range_info [@key "oneHundredS"]
    }
  [@@deriving to_yojson]

  type listing = t list [@@deriving to_yojson]

  let empty_range_info () =
    { count = 0; mean_time = 0.0; max_time = 0.0; total_time = 0.0 }

  let empty_entry identity =
    { identity
    ; count = 0
    ; total_time = 0.0
    ; one_to_ten_us = empty_range_info ()
    ; ten_to_one_hundred_us = empty_range_info ()
    ; one_hundred_us_to_one_ms = empty_range_info ()
    ; one_to_ten_ms = empty_range_info ()
    ; ten_to_one_hundred_ms = empty_range_info ()
    ; one_hundred_ms_to_one_s = empty_range_info ()
    ; one_to_ten_s = empty_range_info ()
    ; ten_to_one_hundred_s = empty_range_info ()
    ; one_hundred_s = empty_range_info ()
    }

  let ten_us = 0.00001

  let one_hundred_us = 0.0001

  let one_ms = 0.001

  let ten_ms = 0.01

  let one_hundred_ms = 0.1

  let one_s = 1.0

  let ten_s = 10.0

  let one_hundred_s = 100.0

  let range_for_duration record duration =
    let open Float in
    if duration < ten_us then record.one_to_ten_us
    else if duration < one_hundred_us then record.ten_to_one_hundred_us
    else if duration < one_ms then record.one_hundred_us_to_one_ms
    else if duration < ten_ms then record.one_to_ten_ms
    else if duration < one_hundred_ms then record.ten_to_one_hundred_ms
    else if duration < one_s then record.one_hundred_ms_to_one_s
    else if duration < ten_s then record.one_to_ten_s
    else if duration < one_hundred_s then record.ten_to_one_hundred_s
    else record.one_hundred_s

  let record ~store identity duration =
    let record =
      Hashtbl.find_or_add store identity ~default:(fun () ->
          empty_entry identity )
    in
    let range = range_for_duration record duration in
    record.count <- record.count + 1 ;
    record.total_time <- record.total_time +. duration ;
    range.count <- range.count + 1 ;
    range.total_time <- range.total_time +. duration ;
    range.max_time <- Float.max range.max_time duration ;
    let f_count = Float.of_int range.count in
    range.mean_time <-
      range.mean_time +. ((duration -. range.mean_time) /. f_count) ;
    ()
end
