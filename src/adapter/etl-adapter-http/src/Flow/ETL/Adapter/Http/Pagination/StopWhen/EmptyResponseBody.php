<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

use function count;

final class EmptyResponseBody extends StopWhen
{
    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return count($response->body()) === 0;
    }
}
