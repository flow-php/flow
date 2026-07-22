<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Sort;

use Flow\ETL\Config\Sort\MemorySortConfig;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\memory_sort;
use function Flow\Filesystem\DSL\fstab;

final class MemorySortBuilderTest extends FlowTestCase
{
    public function test_builds_a_memory_sort_config(): void
    {
        static::assertInstanceOf(MemorySortConfig::class, memory_sort()->build(fstab(), Path::realpath(__DIR__)));
    }
}
