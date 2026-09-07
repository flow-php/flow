<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class CollectingExtractor implements Extractor, OverridingExtractor, RewindableExtractor
{
    private ?Schema $schema = null;

    public function __construct(
        private Extractor $extractor,
    ) {}

    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema;

        $collectedRows = null;

        foreach ($this->extractor->extract($context) as $rows) {
            if ($schema !== null) {
                $rows = $rows->matchTo($schema);
            }

            $collectedRows = $collectedRows === null ? $rows : $collectedRows->merge($rows);
        }

        yield $collectedRows ?? new Rows($this->schema());
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function extractors(): array
    {
        return [$this->extractor];
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        return $this->extractor->schema();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
