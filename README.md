# Internal trace consumer

## Build

```
opam switch create . 4.14.0
# Follow instructions given by above command to enable the environment
# with `eval $(opam env)` if necessary then continue with:
opam switch import opam.export
dune build src/internal_trace_consumer.exe
```

## Usage

```
./_build/default/src/internal_trace_consumer.exe serve \
  --trace-file /path/to/internal_trace.jsonl
```

or

```
dune exec ./src/internal_trace_consumer.exe serve \
  --trace-file /path/to/internal_trace.jsonl
```

which will rebuild the program before executing it.

This will expose a GraphQL server in `http://localhost:9080/graphql`.

## Docker images

### Building

```
docker build . -t internal-trace-consumer:latest
```

### Running

Assuming the main trace file is in `path/to/internal-traces/internal-trace.jsonl`:

```
docker run \
  -p 9080:9080 \
  -v path/to/internal-traces:/traces \
  internal-trace-consumer:latest
```

will run the internal trace consumer and expose the GraphQL server in `http://localhost:9080/graphql`.
