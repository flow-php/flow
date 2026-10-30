<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Breaks exactly one clause of the batch contract: whatever its batch size, it never puts more than
 * one row in a batch.
 */
final class OneRowBatchesExtractor implements BatchableExtractor, Extractor
{
    private BatchSize $batchSize;

    public function __construct(
        private readonly Rows $rows,
    ) {
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        for ($i = 0; $i < $this->rows->count(); $i++) {
            $signal = yield $this->rows->slice($i, 1);

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function schema(): Schema
    {
        return $this->rows->schema();
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }
}
