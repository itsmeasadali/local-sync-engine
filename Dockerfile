FROM rust:1.88-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --locked --release -p sync-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /source/target/release/sync-server /usr/local/bin/sync-server
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["sync-server"]
