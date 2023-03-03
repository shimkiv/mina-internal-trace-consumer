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

