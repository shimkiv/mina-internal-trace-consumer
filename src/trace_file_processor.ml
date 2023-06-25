open Core
open Async
module Block_tracing = Block_tracing

module Make (Handler : sig
  val process_checkpoint : string -> float -> unit Deferred.t

  val process_control :
       other:(string * Yojson.Safe.t) list
    -> string
    -> Yojson.Safe.t
    -> unit Deferred.t

  val file_changed : unit -> unit

  val eof_reached : unit -> unit Deferred.t
end) =
struct
  let last_rotate_end_timestamp = ref 0.0

  let process_event original yojson =
    match yojson with
    | `List [ `String checkpoint; `Float timestamp ] ->
        let%map () = Handler.process_checkpoint checkpoint timestamp in
        true
    | `Assoc [ ("rotated_log_end", `Float timestamp) ] ->
        last_rotate_end_timestamp := timestamp ;
        return false
    | `Assoc ((head, data) :: other) ->
        let%map () = Handler.process_control ~other head data in
        true
    | _ ->
        eprintf "[WARN] unexpected: %s\n%!" original ;
        return true

  let process_log_rotated_start original yojson =
    match yojson with
    | `Assoc [ ("rotated_log_start", `Float timestamp) ] ->
        if Float.(timestamp >= !last_rotate_end_timestamp) then true
        else (
          eprintf "[WARN] file rotatation issued but file didn't rotate\n%!" ;
          false )
    | _ ->
        eprintf "[WARN] expected rotated_log_start, but got: %s\n%!" original ;
        false

  let process_line ~rotated line =
    try
      let yojson = Yojson.Safe.from_string line in
      if rotated then return (process_log_rotated_start line yojson)
      else process_event line yojson
    with _ ->
      eprintf "[ERROR] could not parse line: %s\n%!" line ;
      return true

  let file_changed inode filename =
    try
      let stat = Core.Unix.stat filename in
      inode <> stat.st_ino
    with Unix.Unix_error _ ->
      eprintf "File '%s' removed\n%!" filename ;
      true

  let really_read_line ~inode ~filename ~wait_time reader =
    let pending = ref "" in
    let rec loop () =
      let%bind result =
        Reader.read_until reader (`Char '\n') ~keep_delim:false
      in
      match result with
      | `Eof ->
          if file_changed inode filename then return `File_changed
          else return `Eof_reached
      | `Eof_without_delim data ->
          pending := !pending ^ data ;
          let%bind () = Clock.after wait_time in
          loop ()
      | `Ok line ->
          let line = !pending ^ line in
          pending := "" ;
          return (`Line line)
    in
    loop ()

  let rec process_reader ~inode ~stop_on_eof ~rotated ~filename reader =
    let%bind next_line =
      really_read_line ~inode ~filename ~wait_time:(Time.Span.of_sec 0.2) reader
    in
    match next_line with
    | `Eof_reached ->
        let%bind () =
          if not stop_on_eof then Handler.eof_reached () else Deferred.unit
        in
        let%bind () = Clock.after (Time.Span.of_sec 0.2) in
        if stop_on_eof then return `Eof_reached
        else process_reader ~inode ~stop_on_eof ~rotated ~filename reader
    | `Line line ->
        if%bind process_line ~rotated line then
          process_reader ~inode ~stop_on_eof ~rotated:false ~filename reader
        else return `File_rotated
    | `File_changed ->
        return `File_changed

  let process_file ?(without_rotation = false) filename =
    let rec loop rotated =
      let%bind result =
        try_with (fun () ->
            Reader.with_file filename
              ~f:
                (process_reader ~inode:(Core.Unix.stat filename).st_ino
                   ~stop_on_eof:without_rotation ~rotated ~filename ) )
      in
      match result with
      | Ok `File_rotated when without_rotation ->
          printf "Done processing rotated file %s\n%!" filename ;
          Deferred.unit
      | Ok `Eof_reached ->
          printf "Done processing rotated file %s\n%!" filename ;
          Deferred.unit
      | Ok `File_rotated ->
          printf "File rotated, re-opening %s\n%!" filename ;
          let%bind () = Clock.after (Time.Span.of_sec 2.0) in
          loop true
      | Ok `File_changed ->
          Handler.file_changed () ;
          last_rotate_end_timestamp := 0.0 ;
          printf "File changed, re-opening %s\n%!" filename ;
          let%bind () = Clock.after (Time.Span.of_sec 2.0) in
          loop false
      | Error exn ->
          eprintf
            "File '%s' could not be opened, retrying after 5 seconds. Reason:\n\
             %s\n\
             %!"
            filename (Exn.to_string exn) ;
          let%bind () = Clock.after (Time.Span.of_sec 5.0) in
          loop rotated
    in
    printf "Begin processing trace file: %s\n%!" filename ;
    loop false

  let process_roated_files filename =
    Deferred.List.iter ~how:`Sequential (List.range 0 100) ~f:(fun n ->
        let filename_n = sprintf "%s.%d" filename n in
        try
          let _stat = Core.Unix.stat filename_n in
          process_file ~without_rotation:true filename_n
        with Core.Unix.Unix_error _ -> Deferred.unit )
end
