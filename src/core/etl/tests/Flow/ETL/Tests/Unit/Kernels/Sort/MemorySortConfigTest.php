<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort;

use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Tests\FlowTestCase;

final class MemorySortConfigTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        static::assertSame([], (new MemorySortConfig())->describe()?->data);
    }
}
