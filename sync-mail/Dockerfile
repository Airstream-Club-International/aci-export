# Chef stage - base image with cargo-chef installed
FROM lukemathwalker/cargo-chef:latest-rust-1.92 AS chef
WORKDIR /app

# Planner stage - analyze dependencies and create recipe
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY ddb ./ddb
COPY mailchimp ./mailchimp
COPY db ./db
COPY sync-mail ./sync-mail
COPY sync-app ./sync-app
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage - cook dependencies (cached), then build app
FROM chef AS builder

# Install dependencies for building
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Cook dependencies - this layer is cached until Cargo.toml/lock change
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy source and build - only this runs on code changes
COPY Cargo.toml Cargo.lock ./
COPY ddb ./ddb
COPY mailchimp ./mailchimp
COPY db ./db
COPY sync-mail ./sync-mail
COPY sync-app ./sync-app
RUN cargo build --release --bin sync-mail

# Runtime stage
FROM debian:trixie-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/sync-mail /app/sync-mail

CMD ["/app/sync-mail", "run"]
