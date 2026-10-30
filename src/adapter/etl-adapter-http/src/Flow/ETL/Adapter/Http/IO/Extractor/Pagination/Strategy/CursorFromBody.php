<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\RequestOption;
use Psr\Http\Message\RequestInterface;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;

final class CursorFromBody implements PaginationStrategy
{
    public function __construct(
        private readonly string $cursorPath,
        private readonly RequestOption $inject,
    ) {}

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $base;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        $body = $previous->body();

        if (!array_dot_exists($body, $this->cursorPath)) {
            return null;
        }

        /** @var array<mixed>|scalar|null $cursor */
        $cursor = array_dot_get($body, $this->cursorPath);

        if ($cursor === null || $cursor === '') {
            return null;
        }

        return $this->inject->apply($base, $cursor);
    }
}
