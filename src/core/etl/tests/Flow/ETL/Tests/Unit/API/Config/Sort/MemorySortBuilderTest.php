<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Config\Sort;

use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\memory_sort;

final class MemorySortBuilderTest extends FlowTestCase
{
    public function test_builds_a_memory_sort_config(): void
    {
        static::assertInstanceOf(MemorySortConfig::class, memory_sort()->build(
            Path::realpath(__DIR__),
            new PhpBackend(),
        ));
    }
}
