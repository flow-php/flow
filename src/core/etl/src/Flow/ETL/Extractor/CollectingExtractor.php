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

    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $schema = $this->schema;

        $collectedRows = null;

        foreach ($this->extractor->extract($context) as $rows) {
            if ($schema !== null) {
                $rows = $rows->matchTo($schema, $context->backend());
            }

            // an empty part adds nothing, whatever its schema
            $collectedRows = match (true) {
                $collectedRows === null, $collectedRows->isEmpty() => $rows,
                $rows->isEmpty() => $collectedRows,
                default => $collectedRows->concat($context->backend(), $rows),
            };
        }

        yield $collectedRows ?? Rows::empty($this->schema(), $context->backend());
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

    public function statistics(): Statistics
    {
        return $this->extractor->statistics();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
