<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class RowsExtractor implements Extractor
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

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rows as $rows) {
            if ($this->schema !== null) {
                $rows = array_to_rows($rows->toArray(), $context->hydrator(), $this->schema);
            }

            $signal = yield $rows;

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
