<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;
use Throwable;

final class ClosureThrowingLoader implements Closure, Loader
{
    public int $loadsCount = 0;

    public function __construct(
        private readonly Throwable $throwable,
    ) {}

    public function closure(FlowContext $context): void
    {
        throw $this->throwable;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadsCount++;
    }
}
