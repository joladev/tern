FROM rust:1.99.1-slim-trixie AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make perl
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p server -p mock
RUN mkdir /data

FROM gcr.io/distroless/cc-debian13@sha256:58e12be899275afaa05f1b03051ab19037ed7f252528e57a3a9ad7e220a62578 AS mock
COPY --from=build /src/target/release/mock /usr/local/bin/mock
COPY fixtures /fixtures

FROM gcr.io/distroless/cc-debian13@sha256:58e12be899275afaa05f1b03051ab19037ed7f252528e57a3a9ad7e220a62578
COPY --from=build /src/target/release/server /usr/local/bin/tern
COPY --from=build --chown=65532:65532 /data /data
USER nonroot
CMD ["tern", "serve"]
