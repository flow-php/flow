<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Double;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PaginationStrategy;
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
