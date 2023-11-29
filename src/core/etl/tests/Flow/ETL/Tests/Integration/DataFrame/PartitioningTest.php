<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class PartitioningTest extends IntegrationTestCase
{
    public function test_filter_partitions() : void
    {
        $partitionedRows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        ))
            ->partitionBy('country')
            ->filterPartitions(ref('country')->equals(lit('US')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            ),
            $partitionedRows
        );
    }

    public function test_partition_by() : void
    {
        $rows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        ))
            ->partitionBy(ref('country'))
            ->batchSize(2) // split each partition into two
            ->get();

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                    Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                    Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                    Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                    Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
                ),
            ],
            \iterator_to_array($rows)
        );
    }
}
