<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Flow\ETL\Adapter\Http\Pagination\StopWhen\AllOf;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\AnyOf;

abstract class StopWhen
{
    abstract public function shouldStop(DecodedResponse $response, PageState $state): bool;

    final public function and(self $other): self
    {
        return new AllOf($this, $other);
    }

    final public function or(self $other): self
    {
        return new AnyOf($this, $other);
    }
}
