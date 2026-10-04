<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Yields its batches and reports, for every batch, $bytesPerBatch more native memory held - the batch is modelled as
 * retained for the rest of the run.
 */
final class HoldingExtractor implements Extractor
{
    /**
     * @param list<Rows> $batches
     */
    public function __construct(
        public readonly ReportedMemoryBackend $backend,
        public readonly int $bytesPerBatch,
        public readonly array $batches,
    ) {}

    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        foreach ($this->batches as $batch) {
            $this->backend->reported += $this->bytesPerBatch;

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        return $this->batches[0]->schema();
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
