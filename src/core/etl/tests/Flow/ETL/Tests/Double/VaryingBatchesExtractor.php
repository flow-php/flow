<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Yields the batches it was given, verbatim - each with the schema it was built with. The child a
 * decorator test needs when the point is that batches can disagree with each other.
 */
final class VaryingBatchesExtractor implements Extractor
{
    /**
     * @var array<array-key, Rows>
     */
    private readonly array $batches;

    public function __construct(Rows ...$batches)
    {
        $this->batches = $batches;
    }

    public function extract(FlowContext $context): Generator
    {
        foreach ($this->batches as $batch) {
            yield $batch;
        }
    }

    public function schema(): Schema
    {
        return $this->batches === [] ? new Schema() : $this->batches[0]->schema();
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
