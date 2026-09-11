<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Breaks exactly one clause of the batch contract: whatever its batch size, it never puts more than
 * one row in a batch.
 */
final class OneRowBatchesExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    public function __construct(
        private readonly Rows $rows,
    ) {}

    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rows->all() as $row) {
            $signal = yield Rows::trusted($this->rows->schema(), [$row]);

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

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
