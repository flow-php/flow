ARG FLOW_PHP_VERSION=8.2.11
ARG FLOW_BASE_IMAGE_TAG_SUFFIX=cli-alpine3.18
ARG FLOW_BASE_IMAGE_TAG=${FLOW_PHP_VERSION}-${FLOW_BASE_IMAGE_TAG_SUFFIX}
ARG FLOW_BASE_IMAGE=php:${FLOW_BASE_IMAGE_TAG}

FROM ${FLOW_BASE_IMAGE}

ARG FLOW_VERSION=0.3.3

LABEL maintainer="Norbert Orzechowicz <contact@norbert.tech>"

# Update package list and install dependencies
RUN apk update && apk add --no-cache \
    $PHPIZE_DEPS \
    gmp-dev \
    git

# Install PHP extensions
RUN docker-php-ext-install bcmath gmp

# Build and install the Snappy extension
RUN git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git /tmp/php-ext-snappy \
    && cd /tmp/php-ext-snappy \
    && phpize \
    && ./configure \
    && make \
    && make install \
    && docker-php-ext-enable snappy \
    && rm -rf /tmp/php-ext-snappy

RUN echo "Building image for Flow PHP: ${FLOW_VERSION}" && \
    mkdir /flow-php && \
    cd /flow-php && \
    wget https://github.com/flow-php/flow/releases/download/${FLOW_VERSION}/flow-php.phar

RUN chmod +x /flow-php/flow-php.phar
RUN php -v
RUN php -m
RUN php /flow-php/flow-php.phar --version

ENTRYPOINT ["php", "/flow-php/flow-php.phar"]

VOLUME ["/flow-php"]
WORKDIR /flow-php
