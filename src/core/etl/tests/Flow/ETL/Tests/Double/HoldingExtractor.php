<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
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

    public function extract(Backend $backend, Criteria $criteria): Generator
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

    public function isRepeatable(): bool
    {
        return false;
    }
}
