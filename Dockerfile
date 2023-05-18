ARG BASE_IMAGE=alpine
ARG BASE_IMAGE_VERSION=3.17
ARG BUILD_IMAGE=ocaml/opam
ARG BUILD_IMAGE_VERSION=alpine-3.17-ocaml-4.14
## The above default works if the built machine doesn't change and keeps layers cached.
## Alternatively, `build.Dockerfile` can be used to prepare a builder image to use as a base.
## docker build . -t internal-trace-consumer:build --target builder
# ARG BUILD_IMAGE=internal-trace-consumer
# ARG BUILD_IMAGE_VERSION=build

## Trace consumer program (OCaml)
FROM ${BUILD_IMAGE}:${BUILD_IMAGE_VERSION} AS builder

RUN sudo apk add linux-headers
COPY --chown=opam opam.export .
RUN opam switch import --unlock-base opam.export

FROM builder AS intermediate

WORKDIR /src
COPY --chown=opam:opam . /src/code
RUN cd /src/code && opam exec -- dune build src/internal_trace_consumer.exe

## Remote trace fetcher program (Rust)
FROM rust:1.69.0-alpine${BASE_IMAGE_VERSION} AS rust-builder

WORKDIR /app

RUN apk add --no-cache libgcc libstdc++ openssl openssl-dev musl-dev

COPY ./internal-log-fetcher/Cargo.toml ./internal-log-fetcher/Cargo.lock ./
COPY ./internal-log-fetcher/src ./src
COPY ./internal-log-fetcher/graphql ./graphql

# These RUSTFLAGS are required to properly build an alpine binary
# linked to OpenSSL that doesn't segfault
RUN env RUSTFLAGS="-C target-feature=-crt-static" cargo build --release

## Final image
FROM ${BASE_IMAGE}:${BASE_IMAGE_VERSION} AS app

RUN apk add --no-cache libgcc libstdc++ openssl

COPY ./entrypoint.sh /entrypoint.sh
COPY --from=rust-builder /app/target/release/internal-log-fetcher /internal_log_fetcher
COPY --from=intermediate /src/code/_build/default/src/internal_trace_consumer.exe /internal_trace_consumer

EXPOSE 9080

ENTRYPOINT [ "/entrypoint.sh" ]

CMD [ "consumer", "serve", "--trace-file", "/traces/internal-trace.jsonl", "--port", "9080" ]
