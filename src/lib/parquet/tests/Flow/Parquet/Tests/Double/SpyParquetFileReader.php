<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader\ReadPlan;
use Generator;

/**
 * Answers from a SpyFooter, records close(), every readColumns() call and the chunks read.
 */
final class SpyParquetFileReader implements ParquetFileReader
{
    public int $chunksRead = 0;

    public int $closes = 0;

    public readonly SpyFooter $footer;

    /**
     * @var list<array{list<string>, int, ReadPlan}>
     */
    public array $reads = [];

    /**
     * @param list<array<string, list<mixed>>> $chunks
     */
    public function __construct(
        Schema $schema,
        private readonly array $chunks = [],
        int $rows = 0,
    ) {
        $this->footer = new SpyFooter($schema, $rows);
    }

    public function close(): void
    {
        $this->closes++;
    }

    public function footer(): SpyFooter
    {
        return $this->footer;
    }

    public function readColumns(array $columns, int $batchSize, ReadPlan $plan): Generator
    {
        $this->reads[] = [$columns, $batchSize, $plan];

        foreach ($this->chunks as $chunk) {
            $this->chunksRead++;

            yield $chunk;
        }
    }
}
