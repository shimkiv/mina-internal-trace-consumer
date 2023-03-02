open Core

type t = string [@@deriving equal, compare, hash, yojson, sexp_of]
