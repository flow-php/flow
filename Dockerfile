ARG FLOW_PHP_VERSION=8.5.8
ARG FLOW_DEBIAN_SUITE=trixie
ARG FLOW_BASE_IMAGE=php:${FLOW_PHP_VERSION}-cli-${FLOW_DEBIAN_SUITE}

# Stage 1: Shared base so builder and final stage resolve libpq from the same apt source.
FROM ${FLOW_BASE_IMAGE} AS base

ARG FLOW_DEBIAN_SUITE
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates gnupg curl \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt ${FLOW_DEBIAN_SUITE}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
 && apt-get purge -y --auto-remove gnupg \
 && rm -rf /var/lib/apt/lists/*

# Stage 2: Build stage
FROM base AS builder

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
 && docker-php-ext-install bcmath gmp pdo_mysql pdo_pgsql pdo_sqlite pgsql \
 && pecl install protobuf \
 && docker-php-ext-enable protobuf \
 && curl -L https://github.com/php/pie/releases/latest/download/pie.phar -o /usr/local/bin/pie \
 && chmod +x /usr/local/bin/pie \
 && php /usr/local/bin/pie install kjdev/brotli \
 && php /usr/local/bin/pie install kjdev/lz4 \
 && php /usr/local/bin/pie install kjdev/snappy \
 && php /usr/local/bin/pie install kjdev/zstd

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN php /usr/local/bin/pie install flow-php/arrow-ext:1.x-dev \
 && php /usr/local/bin/pie install flow-php/pg-query-ext:1.x-dev \
 && php /usr/local/bin/pie install flow-php/flow-php-ext:1.x-dev

# Stage 3: Final Image
FROM base AS flow

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
