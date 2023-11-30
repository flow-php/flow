<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;

require __DIR__ . '/../../../bootstrap.php';

$externalProducts = [
    ['id' => 1, 'sku' => 'PRODUCT01'],
    ['id' => 2, 'sku' => 'PRODUCT02'],
    ['id' => 3, 'sku' => 'PRODUCT03'],
];

$internalProducts = [
    ['id' => 2, 'sku' => 'PRODUCT02'],
    ['id' => 3, 'sku' => 'PRODUCT03'],
];

/**
 * DataFrame::join will perform joining having both dataframes in memory.
 * This means that if if the right side dataframe is big (as the left side usually will be a batch)
 * then it might become performance bottleneck.
 * In that case please look at DataFrame::joinEach.
 */
$df = data_frame()
    ->read(from_array($externalProducts))
    ->join(
        data_frame()->read(from_array($internalProducts)),
        Expression::on(['id' => 'id']),
        Join::left_anti
    )
    ->write(to_output());

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();

// Output
//
// +--+---------+
// |id|      sku|
// +--+---------+
// | 1|PRODUCT01|
// +--+---------+
// 1 rows
