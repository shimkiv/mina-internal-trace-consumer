FROM ocaml/opam:alpine-3.17-ocaml-4.14

WORKDIR /src
RUN sudo apk add linux-headers
COPY --chown=opam opam.export .
RUN opam switch import --unlock-base opam.export