<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

final class MaxResults extends StopWhen
{
    public function __construct(
        private readonly int $count,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return $state->resultsFetched >= $this->count;
    }
}
