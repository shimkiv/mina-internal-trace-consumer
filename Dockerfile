ARG BASE_IMAGE=alpine
ARG BASE_IMAGE_VERSION=3.17
ARG BUILD_IMAGE=internal-trace-consumer
ARG BUILD_IMAGE_VERSION=build

FROM ${BUILD_IMAGE}:${BUILD_IMAGE_VERSION} as builder

WORKDIR /src
COPY --chown=opam:opam . /src/code
RUN cd /src/code && opam exec -- dune build internal_trace_consumer.exe

FROM ${BASE_IMAGE}:${BASE_IMAGE_VERSION}

COPY --from=builder /src/code/_build/default/internal_trace_consumer.exe /internal_trace_consumer.exe

EXPOSE 80

ENTRYPOINT [ "/internal_trace_consumer.exe" ]

CMD [ "run", "/traces/internal-trace.jsonl", "80" ]
