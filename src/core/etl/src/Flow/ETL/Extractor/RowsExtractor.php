<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class RowsExtractor implements Extractor, RewindableExtractor
{
    private ?Schema $schema = null;

    /**
     * @var array<Rows>
     */
    private array $rows;

    public function __construct(Rows ...$rows)
    {
        $this->rows = $rows;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        foreach ($this->rows as $rows) {
            $signal = yield $rows->matchTo($schema);

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $schema = new Schema();

        foreach ($this->rows as $rows) {
            $schema = $schema->merge($rows->schema());
        }

        return $schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
