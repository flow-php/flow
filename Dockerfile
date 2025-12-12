# Flow PHP Docker Image
# Uses pre-built base image with all PHP extensions
# Base image is built by .github/workflows/docker-base.yml
ARG FLOW_PHP_VERSION=8.5
ARG FLOW_BASE_IMAGE=ghcr.io/flow-php/flow-base:${FLOW_PHP_VERSION}-alpine

FROM ${FLOW_BASE_IMAGE} AS flow

# Copy the pre-built PHAR file
COPY build/flow.phar /flow-php/flow.phar
RUN chmod +x /flow-php/flow.phar

# Set the work directory, entrypoint, and volume
WORKDIR /flow-php
ENTRYPOINT ["php", "/flow-php/flow.phar"]
VOLUME ["/flow-php"]
