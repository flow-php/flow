<?php

declare(strict_types=1);

/*
 * Writes row_groups_{php,arrow}.parquet: FilterRows through both engines, four row groups of 10 rows, the files
 * FilterParityTest prunes. row_groups_duckdb.parquet comes from ../duckdb.sql. Run with the arrow extension loaded:
 * php generate.php
 */

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\Tests\Context\FilterRows;
use Flow\Parquet\Writer;

require __DIR__ . '/../../../../../../../../../../../vendor/autoload.php';

$options = Options::default()
    ->set(Option::ROW_GROUP_SIZE_BYTES, 1)
    ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, FilterRows::ROWS_PER_GROUP)
    ->set(Option::PAGE_SIZE_BYTES, 1)
    ->set(Option::PAGE_SIZE_CHECK_INTERVAL, FilterRows::ROWS_PER_GROUP)
    ->set(Option::ARROW_WRITE_BATCH_SIZE, FilterRows::ROWS_PER_GROUP);

foreach (['php' => new PhpParquetEngine(), 'arrow' => new RustParquetEngine($options)] as $name => $engine) {
    $path = __DIR__ . '/row_groups_' . $name . '.parquet';

    if (file_exists($path)) {
        unlink($path);
    }

    (new Writer(options: $options, engine: $engine))->write($path, FilterRows::schema(), FilterRows::rows());
}
