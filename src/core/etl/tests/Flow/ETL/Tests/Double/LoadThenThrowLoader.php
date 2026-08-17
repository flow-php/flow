<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Throwable;

final class LoadThenThrowLoader implements Loader
{
    public int $loadsCount = 0;

    public function __construct(
        private readonly Loader $sink,
        private readonly Throwable $throwable,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadsCount++;
        $this->sink->load($rows, $context);

        throw $this->throwable;
    }
}
