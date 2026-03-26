# Stage 1: Build stage
ARG FLOW_PHP_VERSION=8.3.28
ARG FLOW_BASE_IMAGE_TAG_SUFFIX=cli-bookworm
ARG FLOW_BASE_IMAGE_TAG=${FLOW_PHP_VERSION}-${FLOW_BASE_IMAGE_TAG_SUFFIX}
ARG FLOW_BASE_IMAGE=php:${FLOW_BASE_IMAGE_TAG}

FROM ${FLOW_BASE_IMAGE} AS builder

# Install dependencies and PHP extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libgmp-dev \
    libpq-dev \
    libsqlite3-dev \
    default-libmysqlclient-dev \
    libprotobuf-dev \
    libprotobuf-c-dev \
    protobuf-compiler \
    protobuf-c-compiler \
    clang \
    libclang-dev \
 && rm -rf /var/lib/apt/lists/* \
 && docker-php-ext-install bcmath gmp pdo_mysql pdo_pgsql pdo_sqlite \
 && curl -L https://github.com/php/pie/releases/latest/download/pie.phar -o /usr/local/bin/pie \
 && chmod +x /usr/local/bin/pie \
 && php /usr/local/bin/pie install kjdev/brotli \
 && php /usr/local/bin/pie install kjdev/lz4 \
 && php /usr/local/bin/pie install kjdev/snappy \
 && php /usr/local/bin/pie install kjdev/zstd

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN php /usr/local/bin/pie install flow-php/arrow-ext:1.x-dev \
 && php /usr/local/bin/pie install flow-php/pg-query-ext:1.x-dev

# Stage 2: Final Image
FROM ${FLOW_BASE_IMAGE} AS flow

# Install runtime libraries needed by extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libgmp10 \
 && rm -rf /var/lib/apt/lists/*

# Copy the built extensions from the builder stage
COPY --from=builder /usr/local/lib/php/extensions /usr/local/lib/php/extensions
COPY --from=builder /usr/local/etc/php/conf.d /usr/local/etc/php/conf.d

# Copy your PHP application
COPY build/flow.phar /flow-php/flow.phar
RUN chmod +x /flow-php/flow.phar

# Set the work directory, entrypoint, and volume
WORKDIR /flow-php
ENTRYPOINT ["php", "/flow-php/flow.phar"]
VOLUME ["/flow-php"]
