<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use function count;

final class EmptyResponseBody implements StopWhen
{
    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return count($response->body()) === 0;
    }
}
