<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Flow\Azure\SDK\Exception\InvalidArgumentException;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\StreamInterface;

use function is_resource;
use function is_string;
use function stream_get_contents;

final readonly class HttpFactory
{
    public function __construct(
        private RequestFactoryInterface $requestFactory,
        private StreamFactoryInterface $streamFactory,
    ) {}

    public function delete(string $url): RequestInterface
    {
        return $this->requestFactory->createRequest('DELETE', $url);
    }

    public function get(string $url): RequestInterface
    {
        return $this->requestFactory->createRequest('GET', $url);
    }

    public function post(string $url): RequestInterface
    {
        return $this->requestFactory->createRequest('POST', $url);
    }

    public function put(string $url): RequestInterface
    {
        return $this->requestFactory->createRequest('PUT', $url);
    }

    /**
     * @param resource|string $content
     */
    public function stream($content): StreamInterface
    {
        if (!is_string($content) && !is_resource($content)) {
            throw new InvalidArgumentException('Content must be a string or a resource');
        }

        if (is_string($content)) {
            return $this->streamFactory->createStream($content);
        }

        $string = stream_get_contents($content);

        if ($string === false) {
            throw new InvalidArgumentException('Failed to read content from resource');
        }

        return $this->streamFactory->createStream($string);
    }
}
