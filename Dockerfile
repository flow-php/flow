# Stage 1: Build stage
ARG FLOW_PHP_VERSION=8.2.11
ARG FLOW_BASE_IMAGE_TAG_SUFFIX=cli-alpine3.18
ARG FLOW_BASE_IMAGE_TAG=${FLOW_PHP_VERSION}-${FLOW_BASE_IMAGE_TAG_SUFFIX}
ARG FLOW_BASE_IMAGE=php:${FLOW_BASE_IMAGE_TAG}

FROM ${FLOW_BASE_IMAGE} as builder

# Install dependencies and PHP extensions
RUN apk update && apk add --no-cache \
    $PHPIZE_DEPS \
    gmp-dev \
    git \
 && docker-php-ext-install bcmath gmp \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git /tmp/php-ext-snappy \
 && cd /tmp/php-ext-snappy \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && docker-php-ext-enable snappy \
 && rm -rf /tmp/php-ext-snappy

# Stage 2: Final Image
FROM ${FLOW_BASE_IMAGE} as flow

# Copy the built extensions from the builder stage
COPY --from=builder /usr/local/lib/php/extensions /usr/local/lib/php/extensions
COPY --from=builder /usr/local/etc/php/conf.d /usr/local/etc/php/conf.d

# Copy necessary libraries for the extensions
COPY --from=builder /usr/lib/libgmp.so.10 /usr/lib/
COPY --from=builder /usr/lib/libstdc++.so.6 /usr/lib/
COPY --from=builder /usr/lib/libgcc_s.so.1 /usr/lib/

# Copy your PHP application
COPY build/flow.phar /flow-php/flow.phar
RUN chmod +x /flow-php/flow.phar

# Set the work directory, entrypoint, and volume
WORKDIR /flow-php
ENTRYPOINT ["php", "/flow-php/flow.phar"]
VOLUME ["/flow-php"]