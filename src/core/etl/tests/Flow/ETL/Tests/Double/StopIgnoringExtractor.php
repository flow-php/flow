<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_chunk;

/**
 * Breaks exactly one clause of the batch contract: it batches at its size, but never reads what is
 * sent into it, so Signal::STOP does not end it.
 */
final class StopIgnoringExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    public function __construct(
        private readonly Rows $rows,
    ) {}

    public function extract(FlowContext $context): Generator
    {
        foreach (array_chunk($this->rows->all(), $this->batchSize()) as $chunk) {
            yield Rows::trusted($this->rows->schema(), $chunk);
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
