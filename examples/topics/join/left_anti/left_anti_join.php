<?php

declare(strict_types=1);

use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../../bootstrap.php';

$externalProducts = new Rows(
    Row::create(Entry::integer('id', 1), Entry::string('sku', 'PRODUCT01')),
    Row::create(Entry::integer('id', 2), Entry::string('sku', 'PRODUCT02')),
    Row::create(Entry::integer('id', 3), Entry::string('sku', 'PRODUCT03'))
);

$internalProducts = new Rows(
    Row::create(Entry::integer('id', 2), Entry::string('sku', 'PRODUCT02')),
    Row::create(Entry::integer('id', 3), Entry::string('sku', 'PRODUCT03'))
);

/**
 * DataFrame::join will perform joining having both dataframes in memory.
 * This means that if if the right side dataframe is big (as the left side usually will be a batch)
 * then it might become performance bottleneck.
 * In that case please look at DataFrame::joinEach.
 */
$flow = (new Flow())
    ->process($externalProducts)
    ->join(
        (new Flow())->process($internalProducts),
        Expression::on(new Equal('id', 'id')), // by using All or Any comparisons, more than one entry can be used to prepare the condition
        Join::left_anti
    )
    ->write(To::output());

if ('' !== \Phar::running(false)) {
    return $flow;
}

$flow->run();

// Output
//
// +--+---------+
// |id|      sku|
// +--+---------+
// | 1|PRODUCT01|
// +--+---------+
// 1 rows
