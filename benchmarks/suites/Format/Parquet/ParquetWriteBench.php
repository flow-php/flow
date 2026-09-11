<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Parquet;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Generator;
use PhpBench\Attributes as Bench;

use function extension_loaded;

#[Bench\BeforeMethods('warm')]
final class ParquetWriteBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders(['rows', 'engines'])]
    #[Bench\Groups(['format', 'format-parquet'])]
    public function bench_parquet_write(array $params): void
    {
        $engine = $params['engine'] === ArrowParquetEngine::class ? new ArrowParquetEngine() : new PhpParquetEngine();

        (new ParquetWriteScenario((int) $params['rows'], $engine))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function engines(): Generator
    {
        yield 'php' => ['engine' => PhpParquetEngine::class];

        if (extension_loaded('arrow')) {
            yield 'arrow' => ['engine' => ArrowParquetEngine::class];
        }
    }
}
