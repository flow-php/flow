<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

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

    public function extract(FlowContext $context): Generator
    {
        $this->extractCalls++;

        yield from [];
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
}
