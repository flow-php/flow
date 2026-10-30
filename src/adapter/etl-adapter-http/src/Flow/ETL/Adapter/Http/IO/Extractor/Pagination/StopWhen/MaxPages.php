<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

final class MaxPages implements StopWhen
{
    public function __construct(
        private readonly int $pages,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return $state->pagesFetched >= $this->pages;
    }
}
