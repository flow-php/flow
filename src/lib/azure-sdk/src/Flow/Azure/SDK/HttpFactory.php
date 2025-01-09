<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Flow\Azure\SDK\Exception\InvalidArgumentException;
use Psr\Http\Message\{RequestFactoryInterface, RequestInterface, StreamFactoryInterface, StreamInterface};

final readonly class HttpFactory
{
    public function __construct(
        private RequestFactoryInterface $requestFactory,
        private StreamFactoryInterface $streamFactory,
    ) {
    }

    public function delete(string $url) : RequestInterface
    {
        return $this->requestFactory->createRequest('DELETE', $url);
    }

    public function get(string $url) : RequestInterface
    {
        return $this->requestFactory->createRequest('GET', $url);
    }

    public function post(string $url) : RequestInterface
    {
        return $this->requestFactory->createRequest('POST', $url);
    }

    public function put(string $url) : RequestInterface
    {
        return $this->requestFactory->createRequest('PUT', $url);
    }

    /**
     * @param resource|string $content
     */
    public function stream($content) : StreamInterface
    {
        if (!\is_string($content) && !\is_resource($content)) {
            throw new InvalidArgumentException('Content must be a string or a resource');
        }

        if (\is_string($content)) {
            return $this->streamFactory->createStream($content);
        }

        return $this->streamFactory->createStreamFromResource($content);
    }
}
