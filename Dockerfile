ARG FLOW_PHP_VERSION=8.2.11
ARG FLOW_BASE_IMAGE_TAG_SUFFIX=cli-alpine3.18
ARG FLOW_BASE_IMAGE_TAG=${FLOW_PHP_VERSION}-${FLOW_BASE_IMAGE_TAG_SUFFIX}
ARG FLOW_BASE_IMAGE=php:${FLOW_BASE_IMAGE_TAG}

FROM ${FLOW_BASE_IMAGE}

# Install dependencies and PHP extensions
RUN apk update && apk add --no-cache \
    $PHPIZE_DEPS \
    gmp-dev \
    git \
 && docker-php-ext-install bcmath gmp

# Build and install the Snappy extension
RUN git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git /tmp/php-ext-snappy \
    && cd /tmp/php-ext-snappy \
    && phpize \
    && ./configure \
    && make \
    && make install \
    && docker-php-ext-enable snappy \
    && rm -rf /tmp/php-ext-snappy

COPY build/flow.phar /flow-php/flow.phar
CMD ["/flow-php/flow.phar"]

RUN chmod +x /flow-php/flow.phar
RUN php -v
RUN php -m
RUN php /flow-php/flow.phar --version

ENTRYPOINT ["php", "/flow-php/flow.phar"]

VOLUME ["/flow-php"]
WORKDIR /flow-php
