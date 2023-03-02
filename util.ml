let flatten_yojson_variant f v =
  match f v with `List [ tag ] -> tag | _ -> assert false
