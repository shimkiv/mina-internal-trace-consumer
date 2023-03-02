# Internal trace consumer

## Build

```
opam switch create . 4.14.0
# Follow instructions given by above command to enable the environment
# with `eval $(opam env)` if necessary then continue with:
opam switch import opam.export
dune build internal_tracing_processor.exe
```

## Usage

```
# internal_tracing_processor.exe run <trace-file> <port>
./_build/default/internal_tracing_processor.exe run \
  /path/to/internal_trace.jsonl \
  9000
```

or

```
dune exec ./internal_tracing_processor.exe run\
  /path/to/internal_trace.jsonl \
  9000
```

which will rebuild the program before executing it.

