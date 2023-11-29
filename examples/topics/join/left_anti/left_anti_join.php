<?php

declare(strict_types=1);

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_output;
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
    Row::create(int_entry('id', 1), str_entry('sku', 'PRODUCT01')),
    Row::create(int_entry('id', 2), str_entry('sku', 'PRODUCT02')),
    Row::create(int_entry('id', 3), str_entry('sku', 'PRODUCT03'))
);

$internalProducts = new Rows(
    Row::create(int_entry('id', 2), str_entry('sku', 'PRODUCT02')),
    Row::create(int_entry('id', 3), str_entry('sku', 'PRODUCT03'))
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
    ->write(to_output());

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
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
