<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Rows;
use Throwable;

final class ClosureThrowingLoader implements Closure, Discardable, Loader
{
    public int $discarded = 0;

    public int $loadsCount = 0;

    public function __construct(
        private readonly Throwable $throwable,
        private readonly ?Throwable $discardFailure = null,
    ) {}

    public function closure(FlowContext $context): void
    {
        throw $this->throwable;
    }

    public function discard(FlowContext $context): void
    {
        $this->discarded++;

        if ($this->discardFailure !== null) {
            throw $this->discardFailure;
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadsCount++;
    }
}
