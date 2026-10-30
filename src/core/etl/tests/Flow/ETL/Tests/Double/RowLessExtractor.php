<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\from_rows;

/**
 * A source that describes itself and then yields no batch at all - the shape an inferring reader has when the file
 * holds a header and nothing else. Counts its extractions, so a caller can prove the pipeline ran once.
 */
final class RowLessExtractor implements Extractor
{
    public int $extractCalls = 0;

    public function __construct(
        private Schema $schema,
    ) {}

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $this->extractCalls++;

        return from_rows()->extract($backend, new Criteria());
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

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
