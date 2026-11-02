<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function array_values;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class ProjectionPushdownTest extends FlowTestCase
{
    public function test_a_group_by_reads_only_the_columns_it_groups_and_aggregates(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');
        $pushed = new CountingFilesystem($memory);
        $plain = new CountingFilesystem($memory);
        $grouped = static fn(DataFrame $df): DataFrame => $df->groupBy('country')->aggregate(sum(ref('amount')));

        static::assertStringContainsString(
            'Columns: country, amount',
            $grouped(df()->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $memory)))
                ->explain()
                ->toString(),
        );
        static::assertSame(
            $grouped(ParquetFilesContext::withoutProjectionPushdown()->read(from_parquet(
                path('memory://orders/a.parquet'),
                filesystem: $plain,
            )))
                ->fetch()
                ->toArray(),
            $grouped(df()->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $pushed)))
                ->fetch()
                ->toArray(),
        );
        static::assertLessThan(ParquetFilesContext::bytesRead($plain), ParquetFilesContext::bytesRead($pushed));
    }

    public function test_a_filter_the_file_proves_true_reads_none_of_its_columns_the_plan_does_not_select(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');
        $onlyIds = new CountingFilesystem($memory);
        $withAmounts = new CountingFilesystem($memory);

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            df()
                ->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $onlyIds))
                ->filter(ref('amount')->greaterThanEqual(lit(0)))
                ->select('id')
                ->fetch()
                ->toArray(),
        );
        df()
            ->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $withAmounts))
            ->filter(ref('amount')->greaterThanEqual(lit(0)))
            ->select('id', 'amount')
            ->fetch();
        static::assertLessThan(ParquetFilesContext::bytesRead($withAmounts), ParquetFilesContext::bytesRead($onlyIds));
    }

    public function test_a_filter_evaluated_on_rows_returns_only_the_columns_the_plan_selects(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');

        static::assertSame(
            [['id' => 2], ['id' => 3]],
            df()
                ->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $memory))
                ->filter(ref('amount')->greaterThan(lit(10)))
                ->select('id')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_count_over_a_filter_the_file_answers_counts_the_rows_it_keeps(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');

        static::assertSame(
            2,
            df()
                ->read(from_parquet(path('memory://orders/a.parquet'), filesystem: $memory))
                ->filter(ref('amount')->greaterThan(lit(10)))
                ->count(),
        );
    }

    public function test_a_pushed_read_keeps_the_partition_column_it_selects(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://part/year=2024/a.parquet');
        $read = static fn(): ParquetExtractor => from_parquet(path('memory://part/**/*.parquet'), filesystem: $memory);
        $selected = df()->read($read())->select('id', 'year');

        static::assertStringContainsString('Columns: id, year', $selected->explain()->toString());
        static::assertSame(
            ParquetFilesContext::withoutProjectionPushdown()->read($read())->select('id', 'year')->fetch()->toArray(),
            $selected->fetch()->toArray(),
        );
        static::assertSame([1, '2024'], array_values($selected->fetch()->toArray()[0]));
    }

    public function test_a_column_the_pushed_columns_leave_out_fails_as_missing_instead_of_reading_as_null(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"country"');

        foreach (FlowTestCase::extracted(
            from_parquet(path('memory://orders/a.parquet'), filesystem: $memory),
            flow_context(config())->backend(),
            new Criteria(columns: ['id']),
        ) as $rows) {
            $rows->column('country');
        }
    }

    public function test_a_partitioned_read_with_metadata_columns_carries_the_file_uri_and_the_partition_once(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://part/year=2024/a.parquet');

        static::assertSame(
            [
                'id' => 1,
                'country' => 'PL',
                'amount' => 10,
                'note' => 'a',
                '_input_file_uri' => 'memory://part/year=2024/a.parquet',
                'year' => '2024',
            ],
            df()
                ->read(from_parquet(path('memory://part/**/*.parquet'), filesystem: $memory)->withMetadataColumns(true))
                ->fetch()
                ->toArray()[0],
        );
    }

    public function test_a_pushed_read_keeps_the_file_uri_it_selects(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://part/year=2024/a.parquet');
        $read = static fn(): ParquetExtractor => from_parquet(
            path('memory://part/**/*.parquet'),
            filesystem: $memory,
        )->withMetadataColumns(true);
        $selected = df()->read($read())->select('id', '_input_file_uri');

        static::assertStringContainsString('Columns: id, _input_file_uri', $selected->explain()->toString());
        static::assertSame(
            ParquetFilesContext::withoutProjectionPushdown()
                ->read($read())
                ->select('id', '_input_file_uri')
                ->fetch()
                ->toArray(),
            $selected->fetch()->toArray(),
        );
    }

    public function test_a_select_of_a_column_the_file_lacks_fails_as_it_does_without_the_push(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');
        $read = static fn(): ParquetExtractor => from_parquet(path('memory://orders/a.parquet'), filesystem: $memory);

        static::assertSame(
            ParquetFilesContext::outcome(
                static fn(): array => ParquetFilesContext::withoutProjectionPushdown()
                    ->read($read())
                    ->select('typo')
                    ->fetch()
                    ->toArray(),
            ),
            ParquetFilesContext::outcome(static fn(): array => df()->read($read())->select('typo')->fetch()->toArray()),
        );
    }

    public function test_a_column_some_files_of_a_glob_lack_is_null_filled_as_it_is_without_the_push(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://mixed/a.parquet');
        df()
            ->read(from_array([['id' => 9, 'amount' => 90]]))
            ->write(to_parquet(path('memory://mixed/b.parquet'), filesystem: $memory))
            ->run();
        $read = static fn(): ParquetExtractor => from_parquet(
            path('memory://mixed/*.parquet'),
            filesystem: $memory,
        )->unionByName();

        static::assertSame(
            ParquetFilesContext::withoutProjectionPushdown()
                ->read($read())
                ->select('id', 'country')
                ->fetch()
                ->toArray(),
            df()->read($read())->select('id', 'country')->fetch()->toArray(),
        );
    }

    public function test_an_explicit_column_list_still_bounds_what_the_push_reads(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::orders($memory, 'memory://orders/a.parquet');
        $read = static fn(): ParquetExtractor => from_parquet(
            path('memory://orders/a.parquet'),
            columns: ['id', 'country'],
            filesystem: $memory,
        );

        static::assertSame(
            ParquetFilesContext::withoutProjectionPushdown()->read($read())->select('country')->fetch()->toArray(),
            df()->read($read())->select('country')->fetch()->toArray(),
        );
        static::assertSame(
            ParquetFilesContext::outcome(
                static fn(): array => ParquetFilesContext::withoutProjectionPushdown()
                    ->read($read())
                    ->select('amount')
                    ->fetch()
                    ->toArray(),
            ),
            ParquetFilesContext::outcome(
                static fn(): array => df()->read($read())->select('amount')->fetch()->toArray(),
            ),
        );
    }
}
