<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filesystem;

use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;
use Flow\Types\Type\AutoCaster;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;

final class ScalarFunctionFilterTest extends FlowTestCase
{
    public function test_partition_filter_with_cast_rejects_non_matching_files(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->cast('int')->equals(lit(2024)),
            new AutoCaster(),
            flow_context(config()),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }
}
