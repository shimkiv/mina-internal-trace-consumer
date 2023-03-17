open Core
open Async
module Graphql_cohttp_async =
  Graphql_internal.Make (Graphql_async.Schema) (Cohttp_async.Io)
    (Cohttp_async.Body)

module Queries = struct
  open Graphql_async
  open Schema

  module JSON = struct
    type t = Yojson.Basic.t

    let parse = Base.Fn.id

    let serialize = Base.Fn.id

    let typ () = scalar "JSON" ~doc:"Arbitrary JSON" ~coerce:serialize
  end

  let json_type : (unit, JSON.t option) typ = JSON.typ ()

  let get_block_trace =
    field "blockTrace" ~doc:"Block trace" ~typ:json_type
      ~args:Arg.[ arg "block_identifier" ~typ:(non_null string) ]
      ~resolve:(fun _info () block ->
        let trace_rev = Block_tracing.Registry.find_trace block in
        Option.map trace_rev ~f:(fun trace ->
            trace |> Block_tracing.Trace.to_yojson |> Yojson.Safe.to_basic ) )

  let get_block_structured_trace =
    field "blockStructuredTrace" ~doc:"Block structured trace" ~typ:json_type
      ~args:Arg.[ arg "block_identifier" ~typ:(non_null string) ]
      ~resolve:(fun _info () block ->
        let trace_rev = Block_tracing.Registry.find_trace block in
        Option.map trace_rev ~f:(fun trace ->
            let trace = Block_tracing.Structured_trace.of_flat_trace trace in
            trace |> Block_tracing.Structured_trace.to_yojson
            |> Yojson.Safe.to_basic ) )

  let list_block_traces =
    field "blockTraces" ~doc:"Block with traces" ~typ:(non_null json_type)
      ~args:
        Arg.
          [ arg "maxLength" ~doc:"The maximum number of block traces to return."
              ~typ:int
          ]
      ~resolve:(fun _info () max_length ->
        let traces = Block_tracing.Registry.all_traces ?max_length () in
        Block_tracing.Registry.traces_to_yojson traces |> Yojson.Safe.to_basic
        )

  let get_block_traces_distribution =
    field "blockTracesDistribution" ~doc:"Block trace checkpoints distribution"
      ~typ:(non_null json_type)
      ~args:Arg.([] (* TODOX: add parent checkpoint filter *))
      ~resolve:(fun _info () ->
        let open Block_tracing.Distributions in
        let compare d1 d2 = Float.compare d1.total_time d2.total_time in
        let distributions = all () |> List.sort ~compare in
        listing_to_yojson distributions |> Yojson.Safe.to_basic )

  let commands =
    [ get_block_trace
    ; get_block_structured_trace
    ; list_block_traces
    ; get_block_traces_distribution
    ]
end

let create_graphql_server ~bind_to_address ~schema ~server_description port =
  let graphql_callback =
    Graphql_cohttp_async.make_callback (fun _req -> ()) schema
  in
  Cohttp_async.(
    Server.create_expert
      ~on_handler_error:
        (`Call
          (fun _net exn ->
            printf "Exception while handling REST server request: %s\n%!"
              (Exn.to_string_mach exn) ) )
      (Tcp.Where_to_listen.bind_to bind_to_address (On_port port))
      (fun ~body _sock req ->
        let uri = Cohttp.Request.uri req in
        let lift x = `Response x in
        match Uri.path uri with
        | "/" ->
            let body =
              "This page is intentionally left blank. The graphql endpoint can \
               be found at `/graphql`."
            in
            Server.respond_string ~status:`OK body >>| lift
        | "/graphql" ->
            (* [%log debug] "Received graphql request. Uri: $uri"
               ~metadata:
                 [ ("uri", `String (Uri.to_string uri))
                 ; ("context", `String "rest_server")
                 ] ; *)
            graphql_callback () req body
        | _ ->
            Server.respond_string ~status:`Not_found "Route not found" >>| lift
        ) )
  |> Deferred.map ~f:(fun _ ->
         printf "Created %s at: http://localhost:%i/graphql\n%!"
           server_description port )

let schema =
  Graphql_async.Schema.schema ~mutations:[] ~subscriptions:[] Queries.commands
