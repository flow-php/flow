<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination;

interface StopWhen
{
    public function shouldStop(DecodedResponse $response, PageState $state): bool;
}
