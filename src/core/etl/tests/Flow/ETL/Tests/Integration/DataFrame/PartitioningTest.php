<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;

use function array_map;
use function array_merge;
use function file_exists;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\rows_partitioned;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\partition;
use function iterator_to_array;
use function range;
use function rmdir;
use function sort;
use function str_replace;
use function unlink;
use function usort;

final class PartitioningTest extends FlowIntegrationTestCase
{
    public function test_dropping_partitions(): void
    {
        $rows = df()
            ->read(from_rows(rows_partitioned([
                row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
            ], [
                partition('country', 'PL'),
            ])))
            ->dropPartitions()
            ->fetch();

        static::assertFalse($rows->isPartitioned());
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
            ->partitionBy('date')
            ->write(to_text(__DIR__ . '/Fixtures/Partitioning/overwrite/file.txt')->saveMode(overwrite()))
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

    public function test_partition_by(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            )))
            ->partitionBy(ref('country'))
            ->get();

        static::assertEquals(
            [
                rows_partitioned([
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                ], [
                    partition('country', 'PL'),
                ]),
                rows_partitioned([
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                ], [
                    partition('country', 'US'),
                ]),
            ],
            iterator_to_array($rows),
        );
    }

    public function test_partition_by_partitions_order(): void
    {
        df()
            ->read(from_array(array_merge(...array_map(
                static function (int $i): array {
                    $data = [];

                    $maxItems = generate_random_int(2, 10);

                    for ($d = 0; $d < $maxItems; $d++) {
                        $data[] = [
                            'id' => generate_random_string(),
                            'created_at' => (new DateTimeImmutable('2020-01-01'))->add(
                                new DateInterval('P' . $i . 'D'),
                            )->setTime(
                                generate_random_int(0, 23),
                                generate_random_int(0, 59),
                                generate_random_int(0, 59),
                            ),
                            'value' => generate_random_int(1, 1000),
                        ];
                    }

                    return $data;
                },
                range(1, 10),
            ))))
            ->withEntry('year', ref('created_at')->dateFormat('Y'))
            ->withEntry('month', ref('created_at')->dateFormat('m'))
            ->withEntry('day', ref('created_at')->dateFormat('d'))
            ->partitionBy(ref('year'), ref('day'), ref('month'))
            ->run(function (Rows $rows): void {
                $this->assertSame(
                    [
                        'year',
                        'day',
                        'month', // order is changed on purpose
                    ],
                    array_map(static fn(Partition $p) => $p->name, $rows->partitions()->toArray()),
                );
            });
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
            ->partitionBy('order-year', 'order-month', 'order-name')
            ->drop('order-year', 'order-month', 'order-name')
            ->write(to_text($output . '/{order-year}/{order-month}/{order-name}.txt')->saveMode(overwrite()))
            ->run();

        static::assertFileExists($output . '/2024/03/123456-PL.txt');
        static::assertFileExists($output . '/2024/03/789-DE.txt');
        static::assertFileExists($output . '/2025/01/555-FR.txt');

        df()->read(from_text($output
        . '/{order-year}/{order-month}/{order-name}.txt'))->run(function (Rows $rows): void {
            $this->assertSame(
                ['order-year', 'order-month', 'order-name'],
                array_map(static fn(Partition $p) => $p->name, $rows->partitions()->toArray()),
            );
        });

        df()->read(from_text($output . '/**/*.txt'))->run(function (Rows $rows): void {
            $this->assertFalse($rows->isPartitioned());
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
        static::assertSame(2023, $rows->first()->valueOf('year'));
    }
}
