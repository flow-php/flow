<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

final class MaxResults implements StopWhen
{
    public function __construct(
        private readonly int $count,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return $state->resultsFetched >= $this->count;
    }
}
