<?php

declare(strict_types=1);

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;

require __DIR__ . '/../bootstrap.php';

// Datasets::reset() (benchmarks/bootstrap.php) wipes benchmarks/var and refuses any path that does
// not contain /benchmarks/var. Fixtures live in benchmarks/datasets, so nothing here interacts with it.

$rows = BenchmarkRows::count();
$windowRows = BenchmarkRows::windowCount();

foreach (array_unique([$rows, $windowRows]) as $count) {
    $orders = Datasets::orders($count);

    $fixtures = [
        'parquet' => static fn(): string => $orders->parquet(),
        'floe' => static fn(): string => $orders->floe(),
        'csv' => static fn(): string => $orders->csv(),
        'json' => static fn(): string => $orders->json(),
        'jsonLines' => static fn(): string => $orders->jsonLines(),
        'xml' => static fn(): string => $orders->xml(),
        'excel' => static fn(): string => $orders->excel(),
        'text' => static fn(): string => Datasets::text($count)->path(),
        'sellers' => static fn(): string => Datasets::sellers($count)->parquet(),
    ];

    foreach ($fixtures as $name => $materialise) {
        $start = microtime(true);
        printf("%-9s %-10s %s  %.2fs\n", number_format($count), $name, $materialise(), microtime(true) - $start);
    }
}
