<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\OverridingExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_values;

/**
 * Answers isRepeatable() with whatever the test needs, and optionally wraps other extractors so the
 * Repeatability recursion can be exercised.
 */
final class RepeatableExtractor implements Extractor, OverridingExtractor, RewindableExtractor
{
    /**
     * @var list<Extractor>
     */
    private readonly array $wrapped;

    public function __construct(
        private readonly bool $repeatable,
        Extractor ...$wrapped,
    ) {
        $this->wrapped = array_values($wrapped);
    }

    public function extract(FlowContext $context): Generator
    {
        yield new Rows($this->schema());
    }

    /**
     * @return array<Extractor>
     */
    public function extractors(): array
    {
        return $this->wrapped;
    }

    public function isRepeatable(): bool
    {
        return $this->repeatable;
    }

    public function schema(): Schema
    {
        return new Schema();
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
