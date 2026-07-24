<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\ResultCounter;
use Psr\Http\Message\RequestInterface;

use function count;
use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function is_array;

final class PageNumber implements PaginationStrategy, ResultCounter
{
    public function __construct(
        private readonly RequestOption $inject,
        private readonly int $startPage = 1,
        private readonly ?int $pageSize = null,
        private readonly ?RequestOption $sizeOption = null,
        private readonly bool $injectOnFirstRequest = true,
        private readonly ?string $recordsPath = null,
    ) {}

    public function countResults(DecodedResponse $response): int
    {
        if ($this->recordsPath === null || !array_dot_exists($response->body(), $this->recordsPath)) {
            return 0;
        }

        /** @var array<mixed>|scalar|null $records */
        $records = array_dot_get($response->body(), $this->recordsPath);

        return is_array($records) ? count($records) : 0;
    }

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        $request = $this->withPageSize($base);

        if ($this->injectOnFirstRequest) {
            return $this->inject->apply($request, $this->startPage);
        }

        return $request;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        return $this->inject->apply($this->withPageSize($base), $this->startPage + $state->pagesFetched);
    }

    private function withPageSize(RequestInterface $base): RequestInterface
    {
        if ($this->pageSize !== null && $this->sizeOption !== null) {
            return $this->sizeOption->apply($base, $this->pageSize);
        }

        return $base;
    }
}
