<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;

final readonly class OrdersDataset
{
    public function __construct(
        private int $rows,
    ) {}

    public function parquet(): string
    {
        $path = Paths::datasets() . '/orders_' . $this->rows . '.parquet';

        if (native_local_filesystem()->status(path($path)) === null) {
            data_frame()->read(new FakeRandomOrdersExtractor($this->rows))->write(to_parquet($path))->run();
        }

        return $path;
    }

    public function floe(): string
    {
        $parquetPath = $this->parquet();
        $path = Paths::datasets() . '/orders_' . $this->rows . '.floe';

        if (!Datasets::isStale($path, $parquetPath)) {
            return $path;
        }

        native_local_filesystem()->rm(path($path));

        data_frame()->read(from_parquet($parquetPath))->write(to_floe($path))->run();

        return $path;
    }

    public function csv(): string
    {
        $parquetPath = $this->parquet();
        $path = Paths::datasets() . '/orders_' . $this->rows . '.csv';

        if (!Datasets::isStale($path, $parquetPath)) {
            return $path;
        }

        native_local_filesystem()->rm(path($path));

        data_frame()->read(from_parquet($parquetPath))->write(to_csv($path))->run();

        return $path;
    }

    public function json(): string
    {
        $parquetPath = $this->parquet();
        $path = Paths::datasets() . '/orders_' . $this->rows . '.json';

        if (!Datasets::isStale($path, $parquetPath)) {
            return $path;
        }

        native_local_filesystem()->rm(path($path));

        data_frame()->read(from_parquet($parquetPath))->write(to_json($path))->run();

        return $path;
    }

    public function xml(): string
    {
        $parquetPath = $this->parquet();
        $path = Paths::datasets() . '/orders_' . $this->rows . '.xml';

        if (!Datasets::isStale($path, $parquetPath)) {
            return $path;
        }

        native_local_filesystem()->rm(path($path));

        data_frame()->read(from_parquet($parquetPath))->write(to_xml($path))->run();

        return $path;
    }

    public function excel(): string
    {
        $parquetPath = $this->parquet();
        $path = Paths::datasets() . '/orders_' . $this->rows . '.xlsx';

        if (!Datasets::isStale($path, $parquetPath)) {
            return $path;
        }

        native_local_filesystem()->rm(path($path));

        data_frame()->read(from_parquet($parquetPath))->write(to_excel($path))->run();

        return $path;
    }
}
