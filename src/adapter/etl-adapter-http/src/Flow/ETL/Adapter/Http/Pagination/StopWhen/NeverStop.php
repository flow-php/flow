<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

final class NeverStop extends StopWhen
{
    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        return false;
    }
}
