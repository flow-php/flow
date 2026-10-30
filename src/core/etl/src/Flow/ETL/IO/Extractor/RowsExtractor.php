<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

final class RowsExtractor implements Extractor
{
    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

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
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $schema = $this->describe();

        foreach ($this->rows as $rows) {
            $signal = yield $rows->matchTo($schema, $backend);

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        $schema = $this->describe();

        if ($this->schema === null && $schema->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('from_rows()', 'no row carries a column');
        }

        return $schema;
    }

    private function describe(): Schema
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

    public function statistics(): Statistics
    {
        if ($this->statistics === null) {
            $count = 0;

            foreach ($this->rows as $rows) {
                $count += $rows->count();
            }

            $this->statistics = new Statistics(rows: Cardinality::exact($count));
        }

        return $this->statistics;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
