# Internal trace consumer

## Build

```
opam switch create . 4.14.0
# Follow instructions given by above command to enable the environment
# with `eval $(opam env)` if necessary then continue with:
opam switch import opam.export
dune build internal_trace_consumer.exe
```

## Usage

```
# internal_trace_consumer.exe run <trace-file> <port>
./_build/default/internal_trace_consumer.exe run \
  /path/to/internal_trace.jsonl \
  9000
```

or

```
dune exec ./internal_trace_consumer.exe run\
  /path/to/internal_trace.jsonl \
  9000
```

which will rebuild the program before executing it.

This will expose a GraphQL server in `http://localhost:9000/graphql`.

## Docker images

### Building

First build the builder image:

```
docker build . -f build.Dockerfile -t internal-trace-consumer:build
```

once it is ready, the final image can be built with:

```
docker build . -t internal-trace-consumer:latest
```

### Running

Assuming the main trace file is in `path/to/internal-traces/internal-trace.jsonl`:

```
docker run \
  -p 9000:80 \
  -v path/to/internal-traces:/traces \
  internal-trace-consumer:latest
```

will run the internal trace consumer and expose the GraphQL server in `http://localhost:9000/graphql`.
