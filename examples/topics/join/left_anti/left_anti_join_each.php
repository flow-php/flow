<?php

declare(strict_types=1);

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\To;
use Flow\ETL\Extractor;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../../bootstrap.php';

$apiExtractor = new class implements Extractor {
    public function extract(FlowContext $context) : Generator
    {
        yield new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('sku', 'PRODUCT01')),
            Row::create(Entry::integer('id', 2), Entry::string('sku', 'PRODUCT02')),
            Row::create(Entry::integer('id', 3), Entry::string('sku', 'PRODUCT03'))
        );

        yield new Rows(
            Row::create(Entry::integer('id', 10_001), Entry::string('sku', 'PRODUCT10_001')),
            Row::create(Entry::integer('id', 10_002), Entry::string('sku', 'PRODUCT10_002')),
            Row::create(Entry::integer('id', 10_003), Entry::string('sku', 'PRODUCT10_003'))
        );
    }
};

$dbDataFrameFactory = new class implements DataFrameFactory {
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function from(Rows $rows) : DataFrame
    {
        return (new Flow())
            ->process($this->findRowsInDatabase($rows));
    }

    private function findRowsInDatabase(Rows $rows) : Rows
    {
        // Lets pretend there are 10k more entries in the DB
        $databaseRows = \array_map(
            static fn (int $id) : Row => Row::create(Entry::integer('id', $id), Entry::string('sku', 'PRODUCT' . $id)),
            \range(1, 10_000)
        );

        return (new Rows(...$databaseRows))
            // this would be a database SQL query in real life
            ->filter(fn (Row $row) => \in_array($row->valueOf('id'), $rows->reduceToArray('id'), true));
    }
};

/**
 * DataFrame::joinEach in some cases might become more optimal choice, especially when
 * right size is much bigger then a left side. In that case it's better to reduce the ride side
 * by fetching from the storage only what is relevant for the left side.
 */
$flow = (new Flow())
    ->extract($apiExtractor)
    ->joinEach(
        $dbDataFrameFactory,
        Expression::on(new Equal('id', 'id')), // by using All or Any comparisons, more than one entry can be used to prepare the condition
        Join::left_anti
    )
    ->write(To::output());

if ('' !== \Phar::running(false)) {
    return $flow;
}

$flow->run();

// Output:
//
// +-----+-------------+
// |   id|          sku|
// +-----+-------------+
// |10001|PRODUCT10_001|
// |10002|PRODUCT10_002|
// |10003|PRODUCT10_003|
// +-----+-------------+
// 3 rows
