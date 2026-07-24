<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Psr\Http\Message\RequestInterface;

/**
 * Orchestrates a {@see PaginationStrategy}: stop conditions, `>=400` handling, identical-request loop guard and
 * page-state advancement. Implement {@see PaginationStrategy} to add a new pagination style; drop to
 * {@see \Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory} for fully bespoke request derivation.
 */
final class Paginator
{
    private RequestComparator $comparator;

    private ?RequestInterface $lastRequest = null;

    private PageState $state;

    public function __construct(
        private readonly PaginationStrategy $strategy,
        private readonly StopWhen $stopWhen,
        private readonly bool $stopOnClientError = true,
    ) {
        $this->state = new PageState();
        $this->comparator = new RequestComparator();
    }

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $this->lastRequest = $this->strategy->initialRequest($base);
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous): ?RequestInterface
    {
        if ($this->stopOnClientError && $previous->statusCode() >= 400) {
            return null;
        }

        $this->state = $this->state->advance(
            $this->strategy instanceof ResultCounter ? $this->strategy->countResults($previous) : 0,
        );

        if ($this->stopWhen->shouldStop($previous, $this->state)) {
            return null;
        }

        $next = $this->strategy->nextRequest($base, $previous, $this->state);

        if ($next === null) {
            return null;
        }

        if ($this->lastRequest !== null && $this->comparator->equals($next, $this->lastRequest)) {
            return null;
        }

        return $this->lastRequest = $next;
    }
}
