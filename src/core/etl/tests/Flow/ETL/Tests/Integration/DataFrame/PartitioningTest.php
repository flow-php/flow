<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class PartitioningTest extends IntegrationTestCase
{
    public function test_filter_partitions() : void
    {
        $partitionedRows = read(from_rows(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            )
        ))
            ->partitionBy('country')
            ->filterPartitions(ref('country')->equals(lit('US')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            ),
            $partitionedRows
        );
    }

    public function test_partition_by() : void
    {
        $rows = read(from_rows(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            )
        ))
            ->partitionBy(ref('country'))
            ->batchSize(2) // split each partition into two
            ->get();

        $this->assertEquals(
            [
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20))
                ),
                new Rows(
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                ),
                new Rows(
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40))
                ),
                new Rows(
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                ),
            ],
            \iterator_to_array($rows)
        );
    }
}
