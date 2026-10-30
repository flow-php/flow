<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Filesystem;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\ReportsResolvedOperand;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;

use function array_map;
use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function sort;

final class ScalarFunctionFilterTest extends FlowTestCase
{
    public function test_a_listing_keeps_the_files_of_every_partition_the_function_accepts(): void
    {
        $statuses = iterator_to_array(
            MemoryFiles::with([
                'memory://var/multi_partitions/date=2022-01-01/country=pl/file.txt' => 'a',
                'memory://var/multi_partitions/date=2022-01-02/country=de/file.txt' => 'a',
                'memory://var/multi_partitions/date=2022-01-02/country=pl/file.txt' => 'a',
                'memory://var/multi_partitions/date=2022-01-03/country=de/file.txt' => 'a',
                'memory://var/multi_partitions/date=2022-01-03/country=pl/file.txt' => 'a',
            ])->list(
                path('memory://var/multi_partitions/**/*.txt'),
                new ScalarFunctionFilter(
                    all(
                        ref('country')->equals(lit('pl')),
                        all(
                            ref('date')->cast('date')->greaterThanEqual(lit(new DateTimeImmutable('2022-01-02'))),
                            ref('date')->cast('date')->lessThan(lit(new DateTimeImmutable('2022-01-04'))),
                        ),
                    ),
                    schema(),
                    flow_context()->backend(),
                ),
            ),
            false,
        );
        $uris = array_map(static fn(FileStatus $status): string => $status->path->uri(), $statuses);
        sort($uris);

        static::assertSame(
            [
                'memory://var/multi_partitions/date=2022-01-02/country=pl/file.txt',
                'memory://var/multi_partitions/date=2022-01-03/country=pl/file.txt',
            ],
            $uris,
        );
    }

    public function test_a_listing_keeps_the_files_of_the_one_partition_the_function_accepts(): void
    {
        static::assertSame(
            ['memory://var/partitioned/partition_01=b/file_02.txt'],
            array_map(
                static fn(FileStatus $status): string => $status->path->uri(),
                iterator_to_array(
                    MemoryFiles::with([
                        'memory://var/partitioned/partition_01=a/file_01.txt' => 'a',
                        'memory://var/partitioned/partition_01=b/file_02.txt' => 'b',
                    ])->list(
                        path('memory://var/partitioned/**/*.txt'),
                        new ScalarFunctionFilter(
                            ref('partition_01')->equals(lit('b')),
                            schema(),
                            flow_context()->backend(),
                        ),
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_function_is_the_one_given_not_its_resolved_copy(): void
    {
        $function = ref('year')->equals(lit(2024));

        static::assertSame(
            $function,
            (new ScalarFunctionFilter(
                $function,
                schema(int_schema('year')),
                flow_context(config())->backend(),
            ))->function(),
        );
    }

    public function test_accept_evaluates_the_resolved_function(): void
    {
        static::assertTrue((new ScalarFunctionFilter(
            new ReportsResolvedOperand(ref('year')),
            schema(int_schema('year')),
            flow_context(config())->backend(),
        ))->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
    }

    public function test_a_hive_null_partition_value_against_a_declared_nullable_type(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit(2024)),
            schema(int_schema('year', nullable: true)),
            flow_context(config())->backend(),
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
            flow_context(config())->backend(),
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
            flow_context(config())->backend(),
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
            flow_context(config())->backend(),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }

    public function test_an_undeclared_partition_column_keeps_the_raw_path_value(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->equals(lit('2024')),
            schema(),
            flow_context(config())->backend(),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }

    public function test_partition_filter_with_cast_rejects_non_matching_files(): void
    {
        $filter = new ScalarFunctionFilter(
            ref('year')->cast('int')->equals(lit(2024)),
            schema(),
            flow_context(config())->backend(),
        );

        static::assertTrue($filter->accept(new FileStatus(path('flow-file://data/year=2024/file.csv'), true)));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2025/file.csv'), true)));
    }

    public function test_an_undeclared_null_partition_value_reads_as_null(): void
    {
        $filter = new ScalarFunctionFilter(ref('year')->isNull(), schema(), flow_context(config())->backend());

        static::assertTrue($filter->accept(
            new FileStatus(path('flow-file://data/year=__HIVE_DEFAULT_PARTITION__/f.csv'), true),
        ));
        static::assertFalse($filter->accept(new FileStatus(path('flow-file://data/year=2024/f.csv'), true)));
    }
}
