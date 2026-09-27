<?php

declare(strict_types=1);

namespace Flow\ETL\Rows;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class RowsBuffer
{
    private RowsBuilder $builder;

    /**
     * @param int<1, max> $size
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly Backend $backend,
        private readonly int $size,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->size < 1) {
            throw new InvalidArgumentException('Buffer size must be greater than 0, given: ' . $this->size);
        }

        $this->builder = new RowsBuilder($this->schema, $this->backend);
    }

    /**
     * @param array<array-key, mixed> $row
     *
     * @throws SchemaMismatchException
     */
    public function append(array $row): ?Rows
    {
        $this->builder->append($row);

        return $this->builder->count() >= $this->size ? $this->flush() : null;
    }

    /**
     * @throws SchemaMismatchException
     */
    public function appendFrom(Rows $rows, int $i): ?Rows
    {
        $this->builder->appendFrom($rows, $i);

        return $this->builder->count() >= $this->size ? $this->flush() : null;
    }

    /**
     * A take that overshoots the size is released whole.
     *
     * @param list<int> $indices
     */
    public function appendTake(Rows $rows, array $indices): ?Rows
    {
        $this->builder->appendTake($rows, $indices);

        return $this->builder->count() >= $this->size ? $this->flush() : null;
    }

    public function flush(): ?Rows
    {
        if ($this->builder->count() === 0) {
            return null;
        }

        $batch = $this->builder->finish();
        $this->builder = new RowsBuilder($this->schema, $this->backend);

        return $batch;
    }
}
