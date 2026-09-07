<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_values;

/**
 * Counts how many times extract() was pulled, so a test can prove the plan bind read no row. The
 * batches are held in memory, so replaying them yields the same rows every time.
 */
final class CountingExtractor implements Extractor, RewindableExtractor
{
    public int $extractCalls = 0;

    private readonly Schema $schema;

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(Schema $schema, Rows ...$batches)
    {
        $this->schema = $schema;
        $this->batches = array_values($batches);
    }

    public function extract(FlowContext $context): Generator
    {
        $this->extractCalls++;

        yield from $this->batches;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
