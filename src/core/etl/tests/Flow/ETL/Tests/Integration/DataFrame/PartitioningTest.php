<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\partition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\rows_partitioned;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class PartitioningTest extends IntegrationTestCase
{
    public function test_filter_partitions() : void
    {
        $partitionedRows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->partitionBy('country')
            ->filterPartitions(ref('country')->equals(lit('US')))
            ->fetch();

        $this->assertEquals(
            rows_partitioned(
                [
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                ],
                [
                    partition('country', 'US'),
                ]
            ),
            $partitionedRows
        );
    }

    public function test_partition_by() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->partitionBy(ref('country'))
            ->batchSize(2) // split each partition into two
            ->get();

        $this->assertEquals(
            [
                rows_partitioned(
                    [
                        row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                        row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    ],
                    [
                        partition('country', 'PL'),
                    ]
                ),
                rows_partitioned(
                    [
                        row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                        row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    ],
                    [
                        partition('country', 'PL'),
                    ]
                ),
                rows_partitioned(
                    [
                        row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                        row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    ],
                    [
                        partition('country', 'US'),
                    ]
                ),
                rows_partitioned(
                    [
                        row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                        row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                    ],
                    [
                        partition('country', 'US'),
                    ]
                ),
            ],
            \iterator_to_array($rows)
        );
    }
}
