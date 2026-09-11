<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Extractor;
use Flow\ETL\Memory\ArrayMemory;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_memory;
use function Flow\Floe\DSL\from_floe;

final readonly class SourceExtractor
{
    public function __construct(
        private Source $source,
        private SchemaMode $mode,
        private int $rows,
    ) {}

    public function extractor(): Extractor
    {
        $orders = Datasets::orders($this->rows);

        $extractor = match ($this->source) {
            Source::array => from_array(InMemoryOrders::of($this->rows)),
            Source::csv => from_csv($orders->csv()),
            Source::excel => from_excel($orders->excel()),
            Source::floe => from_floe($orders->floe()),
            Source::json => from_json($orders->json()),
            Source::json_lines => from_json_lines($orders->jsonLines()),
            Source::memory => from_memory(new ArrayMemory(InMemoryOrders::of($this->rows))),
            Source::parquet => from_parquet($orders->parquet()),
        };

        return $this->mode === SchemaMode::declared
            ? $extractor->withSchema(OrdersSchema::of($this->source))
            : $extractor;
    }
}
