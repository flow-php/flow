<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Psr\Http\Message\RequestInterface;

interface PaginationStrategy
{
    public function initialRequest(RequestInterface $base): RequestInterface;

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface;
}
