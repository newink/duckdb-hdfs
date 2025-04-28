FROM registry.access.redhat.com/ubi9/toolbox
USER root

# Install build dependencies
RUN dnf install -y \
    epel-release \
    dnf-plugins-core

RUN dnf config-manager --set-enabled crb

RUN dnf groupinstall -y "Development Tools"

RUN dnf install --allowerasing -y \
    cmake \
    boost-devel \
    protobuf-devel \
    openssl-devel \
    krb5-devel \
    libxml2-devel \
    libuuid-devel \
    git \
    curl \
    autoconf \
    automake \
    libtool \
    gettext \
    ccache

ENV CC="ccache gcc" \
  CXX="ccache g++"  \
  CCACHE_DIR="/root/.ccache" \
  CCACHE_MAXSIZE=1G

# Build and install GSASL from source
WORKDIR /tmp
RUN curl -LO https://ftp.gnu.org/gnu/gsasl/gsasl-1.10.0.tar.gz && \
    tar xf gsasl-1.10.0.tar.gz && \
    cd gsasl-1.10.0 && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig

# Build and install libhdfs3
RUN git clone https://github.com/erikmuttersbach/libhdfs3.git && \
    cd libhdfs3 && \
    mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc) && \
    make install

# Set up workdir for your extension build
WORKDIR /app

# Copy only files needed for 'make pull'
COPY . .

# # Run 'make pull' and pin the commit
# RUN make pull && \
#     cd duckdb && \
#     git reset --hard 8e52ec4

# Now copy the rest of your source code


# Continue with your build
RUN --mount=type=cache,id=ccache,target=/root/.ccache \
--mount=type=cache,id=makebuild,target=/app/build \
make -j release_js  

# Default command
CMD ["/bin/bash"]
