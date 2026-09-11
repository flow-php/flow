<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final readonly class NoopLoader implements Loader
{
    public function load(Rows $rows, FlowContext $context): void {}
}
