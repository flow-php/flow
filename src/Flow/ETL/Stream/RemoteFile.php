<?php

declare(strict_types=1);

namespace Flow\ETL\Stream;

use Flow\ETL\Exception\InvalidArgumentException;

/**
 * @implements FileStream<array{uri: string, options: array<string, mixed>}>
 */
final class RemoteFile implements FileStream
{
    /**
     * @var array{
     *   scheme: string,
     *   host?: string,
     *   port?: int,
     *   user?: string,
     *   pass?: string,
     *   query?: string,
     *   path: string,
     *   fragment?: string
     * }
     */
    private array $urlParts;

    /**
     * @param string $uri
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $uri, private readonly array $options = [])
    {
        $urlParts = \parse_url($uri);

        if (!\is_array($urlParts)) {
            throw new InvalidArgumentException('Invalid remote stream URI');
        }

        if (!\array_key_exists('scheme', $urlParts)) {
            throw new InvalidArgumentException('Stream uri is missing scheme');
        }

        if (!\str_starts_with($urlParts['scheme'], 'flow-')) {
            throw new InvalidArgumentException('Stream scheme must starts with "flow-"');
        }

        if (!\in_array($urlParts['scheme'], \stream_get_wrappers(), true)) {
            throw new InvalidArgumentException("Unknown scheme \"{$urlParts['scheme']}\"");
        }

        if (!\array_key_exists('path', $urlParts)) {
            throw new InvalidArgumentException('Stream uri is missing path');
        }

        $this->urlParts = $urlParts;
    }

    public function __serialize() : array
    {
        return [
            'uri' => $this->uri,
            'options' => $this->options,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->uri = $data['uri'];
        /**
         * @psalm-suppress PropertyTypeCoercion
         * @phpstan-ignore-next-line
         */
        $this->urlParts = \parse_url($this->uri);
        $this->options = $data['options'];
    }

    /**
     * @return array<string, mixed>
     */
    public function options() : array
    {
        return $this->options;
    }

    public function scheme() : string
    {
        return $this->urlParts['scheme'];
    }

    public function uri() : string
    {
        return $this->uri;
    }
}
