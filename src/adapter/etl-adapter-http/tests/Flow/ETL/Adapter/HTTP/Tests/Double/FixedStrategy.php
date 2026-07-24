<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Double;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\PaginationStrategy;
use Psr\Http\Message\RequestInterface;

final class FixedStrategy implements PaginationStrategy
{
    public function __construct(
        private readonly ?RequestInterface $next,
    ) {}

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $base;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        return $this->next;
    }
}
