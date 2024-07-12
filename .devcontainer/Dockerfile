#
# Base setup used for development
#
FROM rust:bookworm

# Development specific instructions go here

# Setup env
ENV PATH=/usr/local/cargo/bin:/home/admin/.local/bin:$PATH
# Rust specific envs
ENV RUST_LOG=debug
ENV RUST_BACKTRACE=1

# Install OS packages
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install openssh-client sudo git curl build-essential wget lsb-release python3-pip \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler libsasl2-dev

# Install additional rust tools
RUN rustup component add rustfmt \
    && rustup component add clippy \
    && rustup update

RUN useradd -G sudo -U -m -s /bin/bash admin \
    && echo "admin ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER admin
