<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function array_map;
use function file_exists;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
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
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function rmdir;
use function sort;
use function str_replace;
use function unlink;
use function usort;

final class PartitioningTest extends FlowIntegrationTestCase
{
    public function test_a_partition_filter_with_a_numeric_literal_binds_over_a_string_partition(): void
    {
        // Comparator::comparable(string, integer) is true, so the bind gate lets a bare integer
        // literal through against an undeclared (string) partition column.
        $glob = __DIR__ . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt';

        static::assertCount(
            7,
            df()
                ->read(from_text($glob))
                ->filterPartitions(ref('year')->between(lit(2020), lit(2025)))
                ->fetch(),
        );
        static::assertCount(
            5,
            df()
                ->read(from_text($glob))
                ->filterPartitions(ref('year')->between(lit(2023), lit(2025)))
                ->fetch(),
        );
    }

    public function test_a_partition_filter_binds_when_the_partition_type_is_declared(): void
    {
        $rows = df()
            ->read(from_text(__DIR__
            . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt')->partitionTypes(
                partition_types(year: type_integer()),
            ))
            ->filterPartitions(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertCount(5, $rows);
    }

    public function test_a_partition_filter_on_an_extractor_without_partition_columns_is_not_gated(): void
    {
        // The same literal throws at bind on from_text(), which declares its partition columns
        $glob = __DIR__ . '/Fixtures/Partitioning/multi_partition_pruning_test/**/*.txt';
        $incomparable = static fn(): ScalarFunction => ref('year')->equals(lit(new DateTimeImmutable('2024-01-01')));

        static::assertCount(0, df()->read(from_path_partitions($glob))->filterPartitions($incomparable())->fetch());
        static::assertCount(0, df()->read(files($glob))->filterPartitions($incomparable())->fetch());
    }

    public function test_a_partition_filter_with_an_incomparable_literal_is_refused_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(string == date)' due to data type mismatch.");

        df()
            ->read(from_text(__DIR__
            . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))
            ->filterPartitions(ref('year')->equals(lit(new DateTimeImmutable('2024-01-01'))));
    }

    public function test_overwrite_save_mode_not_dropping_old_partitions(): void
    {
        if (file_exists(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-03')) {
            unlink(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-03/file.txt');
            rmdir(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-03');
        }

        if (file_exists(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-04')) {
            unlink(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-04/file.txt');
            rmdir(__DIR__ . '/Fixtures/Partitioning/overwrite/date=2024-04-04');
        }

        df()
            ->read(from_array([
                ['date' => '2024-04-03'],
                ['date' => '2024-04-04'],
            ]))
            ->write(
                to_text(__DIR__ . '/Fixtures/Partitioning/overwrite/file.txt')
                    ->saveMode(overwrite())
                    // the committed fixtures carry the date in the body, so this write must too
                    ->partitionBy(partition_by('date')->writeColumns()),
            )
            ->run();

        $partitions = df()->read(from_path_partitions(__DIR__ . '/Fixtures/Partitioning/overwrite/**/*.txt'))->fetch();

        $actualData = $partitions->toArray();
        usort($actualData, static fn(array $a, array $b): int => (string) $a['path'] <=> (string) $b['path']);

        static::assertSame(
            [
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/Partitioning/overwrite/date=2024-04-01/file.txt',
                    'partitions' => ['date' => '2024-04-01'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/Partitioning/overwrite/date=2024-04-02/file.txt',
                    'partitions' => ['date' => '2024-04-02'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/Partitioning/overwrite/date=2024-04-03/file.txt',
                    'partitions' => ['date' => '2024-04-03'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/Partitioning/overwrite/date=2024-04-04/file.txt',
                    'partitions' => ['date' => '2024-04-04'],
                ],
            ],
            $actualData,
        );
        $textRows = df()
            ->read(from_text(__DIR__ . '/Fixtures/Partitioning/overwrite/**/*.txt'))
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
        $batches = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
            )))
            ->repartition(ref('country'))
            ->get();

        $countries = array_map(static fn(Rows $batch): array => $batch->reduceToArray(ref(
            'country',
        )), iterator_to_array($batches));

        usort(
            $countries,
            static fn(array $a, array $b): int => type_string()->assert($a[0]) <=> type_string()->assert($b[0]),
        );

        static::assertSame([['PL', 'PL', 'PL'], ['US', 'US']], $countries);
    }

    public function test_partition_directories_nest_in_declaration_order(): void
    {
        $output = __DIR__ . '/Fixtures/Partitioning/declaration_order';

        df()
            ->read(from_array([['text' => 'a', 'year' => '2024', 'month' => '03', 'day' => '01']]))
            ->write(
                to_text($output . '/out.txt')
                    ->saveMode(overwrite())
                    // order is chosen on purpose: the writer nests in the order it was given, not by name
                    ->partitionBy(partition_by('year', 'day', 'month')),
            )
            ->run();

        static::assertFileExists($output . '/year=2024/day=01/month=03/out.txt');
    }

    public function test_partitioning_by_path_placeholders_only(): void
    {
        $output = __DIR__ . '/Fixtures/Partitioning/placeholders';

        df()
            ->read(from_array([
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '123456-PL', 'text' => 'order 1'],
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '789-DE', 'text' => 'order 2'],
                ['order-year' => '2025', 'order-month' => '01', 'order-name' => '555-FR', 'text' => 'order 3'],
            ]))
            ->write(
                to_text($output . '/{order-year}/{order-month}/{order-name}.txt')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('order-year', 'order-month', 'order-name')),
            )
            ->run();

        static::assertFileExists($output . '/2024/03/123456-PL.txt');
        static::assertFileExists($output . '/2024/03/789-DE.txt');
        static::assertFileExists($output . '/2025/01/555-FR.txt');

        df()->read(from_text($output
        . '/{order-year}/{order-month}/{order-name}.txt'))->run(function (Rows $rows): void {
            // the placeholders put the values in the path, and the read takes them back from it
            $this->assertSame(
                ['text', 'order-month', 'order-name', 'order-year'],
                $rows->schema()->references()->names(),
            );
        });

        df()->read(from_text($output . '/**/*.txt'))->run(function (Rows $rows): void {
            $this->assertSame(['text'], $rows->schema()->references()->names());
        });

        $prunedRows = df()
            ->read(from_text($output . '/{order-year}/{order-month}/{order-name}.txt'))
            ->filterPartitions(ref('order-month')->equals(lit('01')))
            ->fetch();

        static::assertCount(1, $prunedRows);
        static::assertSame(['555-FR'], $prunedRows->reduceToArray('order-name'));
    }

    public function test_pruning_multiple_partitions(): void
    {
        $rows = df()
            ->read(from_text(__DIR__
            . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))
            ->filterPartitions(ref('year')->cast('int')->greaterThanEqual(lit(2023)))
            ->filterPartitions(ref('month')->cast('int')->greaterThanEqual(lit(1)))
            ->filterPartitions(ref('day')->cast('int')->lessThan(lit(3)))
            ->filter(ref('text')->notEquals(lit('something')))
            ->withEntry('day', ref('day')->cast('int'))
            ->collect()
            ->fetch();

        $days = $rows->reduceToArray('day');
        sort($days);
        static::assertCount(2, $rows);
        static::assertSame([1, 2], $days);
    }

    public function test_pruning_single_partition(): void
    {
        $rows = df()
            ->read(from_text(__DIR__
            . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))
            ->filterPartitions(
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
            ->withEntry('year', ref('year')->cast('int'))
            ->groupBy([ref('year')])
            ->aggregate(collect(ref('year')))
            ->fetch();

        static::assertCount(1, $rows);
        static::assertSame(2023, $rows->first()->get('year'));
    }
}
