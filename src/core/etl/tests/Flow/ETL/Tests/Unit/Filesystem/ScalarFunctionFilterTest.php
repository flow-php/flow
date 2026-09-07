<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;

final class ScalarFunctionFilterTest extends FlowTestCase
{
    public function test_a_hive_null_partition_value_against_a_declared_nullable_type(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit(2024)),
            schema(int_schema('year', nullable: true)),
            flow_context(config()),
        );

        static::assertFalse($filter->accept(
            new FileStatus(path('flow-file://data/year=__HIVE_DEFAULT_PARTITION__/f.csv'), true),
        ));
    }

    public function test_a_hive_null_partition_value_against_a_declared_non_nullable_type_throws(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit(2024)),
            schema(int_schema('year')),
            flow_context(config()),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "flow-file://data/year=__HIVE_DEFAULT_PARTITION__/f.csv" declares '
            . 'type integer, but its path value NULL cannot be cast to it.',
        );

        $filter->accept(new FileStatus(path('flow-file://data/year=__HIVE_DEFAULT_PARTITION__/f.csv'), true));
    }

    public function test_a_declared_partition_type_that_the_path_cannot_satisfy_throws(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit(2024)),
            schema(int_schema('year')),
            flow_context(config()),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "flow-file://data/year=2024a/file.csv" declares type integer, '
            . 'but its path value "2024a" cannot be cast to it.',
        );

        $filter->accept(new FileStatus(path('flow-file://data/year=2024a/file.csv'), true));
    }

    public function test_a_declared_partition_type_casts_the_path_value(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit(2024)),
            schema(int_schema('year')),
            flow_context(config()),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }

    public function test_an_undeclared_partition_column_keeps_the_raw_path_value(): void
    {
        $filter = new ScalarFunctionFilter(ref('year')->equals(lit('2024')), schema(), flow_context(config()));

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }

    public function test_partition_filter_with_cast_rejects_non_matching_files(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->cast('int')->equals(lit(2024)),
            schema(),
            flow_context(config()),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }
}
