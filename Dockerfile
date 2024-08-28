#
# Base setup used for development
#
FROM rust:bookworm

# Setup environment
ENV PATH=/usr/local/cargo/bin:/home/admin/.local/bin:/home/admin/.local/share/solana/install/active_release/bin:$PATH
ENV RUST_LOG=debug
ENV RUST_BACKTRACE=1

# Install OS packages
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install openssh-client sudo git curl build-essential wget lsb-release python3-pip nano \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler libsasl2-dev

# Install additional rust tools
RUN rustup component add rustfmt \
    && rustup component add clippy \
    && rustup update

RUN useradd -G sudo -U -m -s /bin/bash admin \
    && echo "admin ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER admin
WORKDIR /home/admin

RUN git clone https://github.com/bwarelabs/solana-cos-plugin.git

WORKDIR /home/admin/solana-cos-plugin

RUN sh -c "$(curl -sSfL https://release.solana.com/v1.18.18/install)"

RUN cargo build

COPY config.json /home/admin/solana-cos-plugin/config.json

ENTRYPOINT ["solana-test-validator", "--geyser-plugin-config", "config.json"]
