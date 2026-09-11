<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;

use function array_slice;
use function Flow\ETL\DSL\data_frame;

/**
 * The orders fixture caps width at 11 columns, so these points are a level, not a scaling curve.
 */
final readonly class PipelineWidthScenario
{
    private const COLUMNS = [
        'order_id',
        'seller_id',
        'created_at',
        'updated_at',
        'cancelled_at',
        'discount',
        'email',
        'customer',
        'address',
        'notes',
        'items',
    ];

    public function __construct(
        private Source $source,
        private int $width,
        private int $rows,
    ) {}

    /**
     * @return list<string>
     */
    public function columns(): array
    {
        return array_slice(self::COLUMNS, 0, $this->width);
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor())
            ->select(...$this->columns())
            ->run();
    }
}
