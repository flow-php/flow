<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Flow\ETL\Exception\RuntimeException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamFactoryInterface;

use function Flow\ArrayDot\array_dot_set;
use function Flow\Types\DSL\type_array;
use function http_build_query;
use function json_decode;
use function json_encode;
use function parse_str;

final class RequestOption
{
    private function __construct(
        public readonly InjectInto $into,
        public readonly string $name,
        private readonly ?StreamFactoryInterface $streamFactory = null,
    ) {}

    public static function bodyPath(string $path, StreamFactoryInterface $streamFactory): self
    {
        return new self(InjectInto::BodyPath, $path, $streamFactory);
    }

    public static function header(string $name): self
    {
        return new self(InjectInto::Header, $name);
    }

    public static function queryParam(string $name): self
    {
        return new self(InjectInto::QueryParam, $name);
    }

    public static function replaceUri(): self
    {
        return new self(InjectInto::ReplaceUri, '');
    }

    public function apply(RequestInterface $base, mixed $value): RequestInterface
    {
        return match ($this->into) {
            InjectInto::QueryParam => $this->applyQueryParam($base, $value),
            InjectInto::Header => $base->withHeader($this->name, (string) $value),
            InjectInto::BodyPath => $this->applyBodyPath($base, $value),
            InjectInto::ReplaceUri => $base->withUri((new RelativeUriResolver())->resolve(
                $base->getUri(),
                (string) $value,
            )),
        };
    }

    private function applyQueryParam(RequestInterface $base, mixed $value): RequestInterface
    {
        $query = [];
        parse_str($base->getUri()->getQuery(), $query);
        $query[$this->name] = (string) $value;

        return $base->withUri($base->getUri()->withQuery(http_build_query($query)));
    }

    private function applyBodyPath(RequestInterface $base, mixed $value): RequestInterface
    {
        if ($this->streamFactory === null) {
            throw new RuntimeException('BodyPath injection requires a PSR-17 StreamFactoryInterface.');
        }

        $body = $base->getBody();

        if ($body->isSeekable()) {
            $body->seek(0);
        }

        $content = $body->getContents();

        $decoded = $content === '' ? [] : type_array()->assert(json_decode($content, true, 512, JSON_THROW_ON_ERROR));

        return $base->withBody($this->streamFactory->createStream(json_encode(
            array_dot_set($decoded, $this->name, $value),
            JSON_THROW_ON_ERROR,
        )));
    }
}
