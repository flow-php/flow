<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

use function count;

/**
 * Accumulates rows and releases them as size-bounded Rows batches.
 */
final class RowsBuffer
{
    /**
     * @var list<Row>
     */
    private array $rows = [];

    /**
     * @param int<1, max> $size
     */
    public function __construct(
        private readonly int $size,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->size < 1) {
            throw new InvalidArgumentException('Buffer size must be greater than 0, given: ' . $this->size);
        }
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

        $batch = new Rows(...$this->rows);
        $this->rows = [];

        return $batch;
    }
}
