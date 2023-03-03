ARG BASE_IMAGE=alpine
ARG BASE_IMAGE_VERSION=3.17
ARG BUILD_IMAGE=ocaml/opam
ARG BUILD_IMAGE_VERSION=alpine-3.17-ocaml-4.14
## The above default works if the built machine doesn't change and keeps layers cached.
## Alternatively, `build.Dockerfile` can be used to prepare a builder image to use as a base.
# ARG BUILD_IMAGE=internal-trace-consumer
# ARG BUILD_IMAGE_VERSION=build

FROM ${BUILD_IMAGE}:${BUILD_IMAGE_VERSION} as builder

RUN sudo apk add linux-headers
COPY --chown=opam opam.export .
RUN opam switch import --unlock-base opam.export

WORKDIR /src
COPY --chown=opam:opam . /src/code
RUN cd /src/code && opam exec -- dune build src/internal_trace_consumer.exe

FROM ${BASE_IMAGE}:${BASE_IMAGE_VERSION}

COPY --from=builder /src/code/_build/default/src/internal_trace_consumer.exe /internal_trace_consumer.exe

EXPOSE 9080

ENTRYPOINT [ "/internal_trace_consumer.exe" ]

CMD [ "serve", "--trace-file", "/traces/internal-trace.jsonl", "--port", "9080" ]
