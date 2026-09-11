<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Closure;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function count;

/**
 * Accumulates rows and releases them as size-bounded Rows batches.
 */
final class RowsBuffer
{
    /**
     * @var Closure(Schema, list<Row>): Rows
     */
    private readonly Closure $batch;

    /**
     * @var list<Row>
     */
    private array $rows = [];

    /**
     * @param int<1, max> $size
     * @param null|Closure(Schema, list<Row>): Rows $batch builds each released batch - Rows::trusted(...) or
     *                                                   Rows::conformed(...) for rows that already passed the gate;
     *                                                   null checks every row through new Rows()
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly int $size,
        ?Closure $batch = null,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->size < 1) {
            throw new InvalidArgumentException('Buffer size must be greater than 0, given: ' . $this->size);
        }

        $this->batch =
            $batch ??
            /**
             * @param list<Row> $rows
             */
            static fn(Schema $schema, array $rows): Rows => new Rows($schema, ...$rows);
    }

    public function add(Row $row): ?Rows
    {
        $this->rows[] = $row;

        if (count($this->rows) < $this->size) {
            return null;
        }

        return $this->flush();
    }

    public function flush(): ?Rows
    {
        if ($this->rows === []) {
            return null;
        }

        $batch = ($this->batch)($this->schema, $this->rows);
        $this->rows = [];

        return $batch;
    }
}
