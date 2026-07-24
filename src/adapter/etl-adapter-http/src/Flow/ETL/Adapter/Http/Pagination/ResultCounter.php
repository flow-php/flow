<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

interface ResultCounter
{
    public function countResults(DecodedResponse $response): int;
}
