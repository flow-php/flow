<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

final class MaxPages extends StopWhen
{
    public function __construct(
        private readonly int $pages,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return $state->pagesFetched >= $this->pages;
    }
}
