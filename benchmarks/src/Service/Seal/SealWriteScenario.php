<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Seal;

use CmsIg\Seal\Schema\Schema as SealSchema;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final readonly class SealWriteScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function index(): string
    {
        return 'benchmark_orders_seal_write_' . $this->rows;
    }

    public function setUp(): void
    {
        Datasets::orders($this->rows)->floe();

        SealEngine::recreateIndex(SealEngine::open($this->schema()), $this->index());
    }

    public function run(): void
    {
        $engine = SealEngine::open($this->schema());

        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->write(to_seal_upsert($engine, $this->index())->withIdentifierEntry('order_id')->withBulkSize(1000))
            ->run();
    }

    public function dropIndex(): void
    {
        SealEngine::dropIndex(SealEngine::open($this->schema()), $this->index());
    }

    private function schema(): SealSchema
    {
        return to_seal_schema((new FakeRandomOrdersExtractor())->schema(), $this->index(), 'order_id');
    }
}
