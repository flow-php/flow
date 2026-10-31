<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Rows;
use Generator;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_column;
use function array_map;
use function array_unique;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function sort;
use function str_replace;
use function usort;

final class PartitioningTest extends FlowIntegrationTestCase
{
    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        $root = $this->cacheDir->path();
        $text = static fn(string $value): Rows => array_to_rows([[
            'text' => $value,
        ]], schema(str_schema('text', nullable: true)));

        FloeFilesContext::writeDailyPartitionedFiles(native_local_filesystem(), $root . '/multi_partitioned');
        FloeFilesContext::writeFiles(native_local_filesystem(), [
            $root . '/rebind/region=eu/tier=gold/a.floe' => $text('eu-gold'),
            $root . '/rebind/region=us/b.floe' => $text('us-plain'),
            $root . '/overwrite/date=2024-04-01/file.floe' => $text('2024-04-01'),
            $root . '/overwrite/date=2024-04-02/file.floe' => $text('2024-04-02'),
        ]);
    }

    public function test_a_partition_filter_with_a_numeric_literal_binds_over_a_string_partition(): void
    {
        // Comparator::comparable(string, integer) is true, so the bind gate lets a bare integer
        // literal through against an undeclared (string) partition column.
        $glob = $this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe';

        static::assertCount(
            7,
            df()
                ->read(from_floe($glob))
                ->filter(ref('year')->between(lit(2020), lit(2025)))
                ->fetch(),
        );
        static::assertCount(
            5,
            df()
                ->read(from_floe($glob))
                ->filter(ref('year')->between(lit(2023), lit(2025)))
                ->fetch(),
        );
    }

    public function test_a_partition_filter_binds_when_the_partition_type_is_declared(): void
    {
        static::assertCount(
            5,
            df()
                ->read(
                    from_floe($this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe')
                        ->partitionTypes(partition_types(year: type_integer())),
                )
                ->filter(ref('year')->equals(lit(2023)))
                ->fetch(),
        );
    }

    #[DataProvider('file_listings')]
    public function test_a_partition_filter_on_a_file_listing_prunes_it(Extractor $extractor): void
    {
        $rows = df()
            ->read($extractor)
            ->filter(ref('year')->equals(lit('2023')))
            ->fetch();

        static::assertCount(5, $rows);
        static::assertSame(['2023'], array_values(array_unique(array_column($rows->toArray(), 'year'))));
    }

    public static function file_listings(): Generator
    {
        $glob = __DIR__ . '/../../Fixtures/multi_partitioned/**/*.txt';

        yield 'from_path_partitions' => [from_path_partitions($glob)];
        yield 'files' => [files($glob)];
    }

    public function test_a_partition_value_that_is_not_a_date_fails_a_date_filter(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "date" type (row 0)');

        df()
            ->read(from_floe($this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe'))
            ->filter(ref('year')->equals(lit(new DateTimeImmutable('2024-01-01'))))
            ->fetch();
    }

    public function test_pruning_keeps_the_source_schema(): void
    {
        // "tier" is nullable because only one of the two listed paths carries it; pruning the read to
        // that path does not change the schema - it is a property of the source, not of the read
        $df = df()->read(from_floe($this->cacheDir->path() . '/rebind/**/*.floe'));

        static::assertTrue($df->schema()->get('tier')->isNullable());

        $df = $df->filter(ref('region')->equals(lit('eu')));

        static::assertTrue($df->schema()->get('tier')->isNullable());
        static::assertEquals($df->schema(), $df->fetch()->schema());
    }

    public function test_overwrite_save_mode_not_dropping_old_partitions(): void
    {
        df()
            ->read(from_array([
                ['date' => '2024-04-03', 'text' => '2024-04-03'],
                ['date' => '2024-04-04', 'text' => '2024-04-04'],
            ]))
            ->write(
                to_floe($this->cacheDir->path() . '/overwrite/file.floe')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('date')),
            )
            ->run();

        $actualData = df()
            ->read(from_path_partitions($this->cacheDir->path() . '/overwrite/**/*.floe'))
            ->fetch()
            ->toArray();
        usort($actualData, static fn(array $a, array $b): int => (string) $a['path'] <=> (string) $b['path']);

        static::assertSame(
            [
                [
                    'path' =>
                        'file://'
                            . str_replace('\\', '/', $this->cacheDir->path())
                            . '/overwrite/date=2024-04-01/file.floe',
                    'partitions' => ['date' => '2024-04-01'],
                    'date' => '2024-04-01',
                ],
                [
                    'path' =>
                        'file://'
                            . str_replace('\\', '/', $this->cacheDir->path())
                            . '/overwrite/date=2024-04-02/file.floe',
                    'partitions' => ['date' => '2024-04-02'],
                    'date' => '2024-04-02',
                ],
                [
                    'path' =>
                        'file://'
                            . str_replace('\\', '/', $this->cacheDir->path())
                            . '/overwrite/date=2024-04-03/file.floe',
                    'partitions' => ['date' => '2024-04-03'],
                    'date' => '2024-04-03',
                ],
                [
                    'path' =>
                        'file://'
                            . str_replace('\\', '/', $this->cacheDir->path())
                            . '/overwrite/date=2024-04-04/file.floe',
                    'partitions' => ['date' => '2024-04-04'],
                    'date' => '2024-04-04',
                ],
            ],
            $actualData,
        );
        $textRows = df()
            ->read(from_floe($this->cacheDir->path() . '/overwrite/**/*.floe'))
            ->fetch()
            ->toArray();
        usort($textRows, static fn(array $a, array $b): int => (string) $a['text'] <=> (string) $b['text']);
        static::assertSame(
            [
                ['text' => '2024-04-01', 'date' => '2024-04-01'],
                ['text' => '2024-04-02', 'date' => '2024-04-02'],
                ['text' => '2024-04-03', 'date' => '2024-04-03'],
                ['text' => '2024-04-04', 'date' => '2024-04-04'],
            ],
            $textRows,
        );
    }

    public function test_repartition_groups_every_row_sharing_a_key_into_one_batch(): void
    {
        $countries = array_map(
            static fn(Rows $batch): array => $batch->reduceToArray(ref('country')->base()),
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL', 'age' => 20],
                            ['id' => 5, 'country' => 'US', 'age' => 40],
                            ['id' => 2, 'country' => 'PL', 'age' => 20],
                            ['id' => 6, 'country' => 'US', 'age' => 40],
                            ['id' => 3, 'country' => 'PL', 'age' => 25],
                        ],
                        schema(int_schema('id'), str_schema('country'), int_schema('age')),
                    )))
                    ->repartition(ref('country'))
                    ->get(),
            ),
        );

        usort(
            $countries,
            static fn(array $a, array $b): int => type_string()->assert($a[0]) <=> type_string()->assert($b[0]),
        );

        static::assertSame([['PL', 'PL', 'PL'], ['US', 'US']], $countries);
    }

    public function test_partition_directories_nest_in_declaration_order(): void
    {
        $output = $this->cacheDir->path() . '/declaration_order';

        df()
            ->read(from_array([['text' => 'a', 'year' => '2024', 'month' => '03', 'day' => '01']]))
            ->write(
                to_floe($output . '/out.floe')
                    ->saveMode(overwrite())
                    // order is chosen on purpose: the writer nests in the order it was given, not by name
                    ->partitionBy(partition_by('year', 'day', 'month')),
            )
            ->run();

        static::assertFileExists($output . '/year=2024/day=01/month=03/out.floe');
    }

    public function test_partitioning_by_path_placeholders_only(): void
    {
        $output = $this->cacheDir->path() . '/placeholders';

        df()
            ->read(from_array([
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '123456-PL', 'text' => 'order 1'],
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '789-DE', 'text' => 'order 2'],
                ['order-year' => '2025', 'order-month' => '01', 'order-name' => '555-FR', 'text' => 'order 3'],
            ]))
            ->write(
                to_floe($output . '/{order-year}/{order-month}/{order-name}.floe')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('order-year', 'order-month', 'order-name')),
            )
            ->run();

        static::assertFileExists($output . '/2024/03/123456-PL.floe');
        static::assertFileExists($output . '/2024/03/789-DE.floe');
        static::assertFileExists($output . '/2025/01/555-FR.floe');

        df()
            ->read(from_floe($output . '/{order-year}/{order-month}/{order-name}.floe'))
            ->forEach(function (Rows $rows): void {
                // the placeholders put the values in the path, and the read takes them back from it
                $this->assertSame(['text', 'order-month', 'order-name', 'order-year'], $rows->schema()->names());
            });

        df()
            ->read(from_floe($output . '/**/*.floe'))
            ->forEach(function (Rows $rows): void {
                $this->assertSame(['text'], $rows->schema()->names());
            });

        $prunedRows = df()
            ->read(from_floe($output . '/{order-year}/{order-month}/{order-name}.floe'))
            ->filter(ref('order-month')->equals(lit('01')))
            ->fetch();

        static::assertCount(1, $prunedRows);
        static::assertSame(['555-FR'], $prunedRows->reduceToArray('order-name'));
    }

    public function test_pruning_multiple_partitions(): void
    {
        $rows = df()
            ->read(from_floe($this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe'))
            ->filter(ref('year')->cast('int')->greaterThanEqual(lit(2023)))
            ->filter(ref('month')->cast('int')->greaterThanEqual(lit(1)))
            ->filter(ref('day')->cast('int')->lessThan(lit(3)))
            ->filter(ref('text')->notEquals(lit('something')))
            ->withColumn('day', ref('day')->cast('int'))
            ->collect()
            ->fetch();

        $days = $rows->reduceToArray('day');
        sort($days);
        static::assertCount(2, $rows);
        static::assertSame([1, 2], $days);
    }

    public function test_a_mixed_predicate_prunes_on_its_partition_conjunct_and_still_filters_rows(): void
    {
        $glob = $this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe';

        static::assertSame(
            df()
                ->read(from_floe($glob))
                ->filter(ref('year')->cast('int')->greaterThanEqual(lit(2023)))
                ->filter(ref('text')->notEquals(lit('something')))
                ->fetch()
                ->toArray(),
            df()
                ->read(from_floe($glob))
                ->filter(
                    ref('year')
                        ->cast('int')
                        ->greaterThanEqual(lit(2023))
                        ->and(ref('text')->notEquals(lit('something'))),
                )
                ->fetch()
                ->toArray(),
        );
    }

    public function test_pruning_single_partition(): void
    {
        $rows = df()
            ->read(from_floe($this->cacheDir->path() . '/multi_partitioned/year=*/month=*/day=*/*.floe'))
            ->filter(
                ref('year')
                    ->cast('string')
                    ->concat(
                        lit('-'),
                        ref('month')->cast('string')->strPadLeft(2, '0'),
                        lit('-'),
                        ref('day')->cast('string')->strPadLeft(2, '0'),
                    )
                    ->cast('date')
                    ->greaterThanEqual(lit(new DateTimeImmutable('2023-01-01'))),
            )
            ->collect()
            ->select('year')
            ->withColumn('year', ref('year')->cast('int'))
            ->groupBy([ref('year')])
            ->aggregate(collect(ref('year')))
            ->fetch();

        static::assertCount(1, $rows);
        static::assertSame(2023, $rows->column('year')->value(0));
    }
}
