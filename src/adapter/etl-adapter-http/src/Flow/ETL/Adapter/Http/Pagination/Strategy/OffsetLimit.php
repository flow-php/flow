<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\ResultCounter;
use Psr\Http\Message\RequestInterface;

final class OffsetLimit implements PaginationStrategy, ResultCounter
{
    public function __construct(
        private readonly RequestOption $offsetOption,
        private readonly RequestOption $limitOption,
        private readonly int $limit,
        private readonly int $startOffset = 0,
        private readonly bool $injectOnFirstRequest = true,
    ) {}

    public function countResults(DecodedResponse $response): int
    {
        return $this->limit;
    }

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        $request = $this->limitOption->apply($base, $this->limit);

        if ($this->injectOnFirstRequest) {
            return $this->offsetOption->apply($request, $this->startOffset);
        }

        return $request;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        return $this->offsetOption->apply(
            $this->limitOption->apply($base, $this->limit),
            $this->startOffset + ($this->limit * $state->pagesFetched),
        );
    }
}
