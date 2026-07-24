<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_integer;

final class TotalReached extends StopWhen
{
    public function __construct(
        private readonly string $totalPath,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        $body = $response->body();

        if (!array_dot_exists($body, $this->totalPath)) {
            return false;
        }

        $total = array_dot_get($body, $this->totalPath, type_integer());

        return $total !== null && $state->resultsFetched >= $total;
    }
}
