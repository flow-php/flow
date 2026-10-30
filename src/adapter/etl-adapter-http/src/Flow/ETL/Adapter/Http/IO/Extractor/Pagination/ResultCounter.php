<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination;

interface ResultCounter
{
    public function countResults(DecodedResponse $response): int;
}
