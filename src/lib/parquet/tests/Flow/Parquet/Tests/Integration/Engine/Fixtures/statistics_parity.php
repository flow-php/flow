<?php

declare(strict_types=1);

/*
 * Writes statistics_parity_{php,arrow}.parquet: StatisticsParityRows through both engines, the files duckdb.sql reads
 * the expected StatisticsParityTest values from. Run with the arrow extension loaded: php statistics_parity.php
 */

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Tests\Context\StatisticsParityRows;
use Flow\Parquet\Writer;

require __DIR__ . '/../../../../../../../../../../vendor/autoload.php';

foreach (['php' => new PhpParquetEngine(), 'arrow' => new RustParquetEngine()] as $name => $engine) {
    $path = __DIR__ . '/statistics_parity_' . $name . '.parquet';

    if (file_exists($path)) {
        unlink($path);
    }

    (new Writer(engine: $engine))->write($path, StatisticsParityRows::schema(), StatisticsParityRows::rows());
}
