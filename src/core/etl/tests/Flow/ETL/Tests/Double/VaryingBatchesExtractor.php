<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

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

    public function extract(Backend $backend, Criteria $criteria): Generator
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

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
