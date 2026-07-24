<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Psr\Http\Message\RequestInterface;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function is_string;

final class NextUrlFromBody implements PaginationStrategy
{
    public function __construct(
        private readonly string $path,
    ) {}

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $base;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        $body = $previous->body();

        if (!array_dot_exists($body, $this->path)) {
            return null;
        }

        /** @var array<mixed>|scalar|null $url */
        $url = array_dot_get($body, $this->path);

        if (!is_string($url) || $url === '') {
            return null;
        }

        return RequestOption::replaceUri()->apply($base, $url);
    }
}
