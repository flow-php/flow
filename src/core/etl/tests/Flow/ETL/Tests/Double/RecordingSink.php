<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Rows;

final class RecordingSink implements Closure, Discardable, Loader
{
    public int $closed = 0;

    public int $discarded = 0;

    public int $loaded = 0;

    public function closure(FlowContext $context): void
    {
        $this->closed++;
    }

    public function discard(FlowContext $context): void
    {
        $this->discarded++;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loaded++;
    }
}
