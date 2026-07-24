<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

final class AnyOf extends StopWhen
{
    /**
     * @var array<StopWhen>
     */
    private array $conditions;

    public function __construct(StopWhen ...$conditions)
    {
        $this->conditions = $conditions;
    }

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        foreach ($this->conditions as $condition) {
            if ($condition->shouldStop($response, $state)) {
                return true;
            }
        }

        return false;
    }
}
