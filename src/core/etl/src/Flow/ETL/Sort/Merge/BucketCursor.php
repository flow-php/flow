<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Generator;

use function array_values;
use function count;

final class BucketCursor
{
    private int $count = 0;

    private int $index = 0;

    /**
     * @var list<Row>
     */
    private array $rows = [];

    private bool $started = false;

    /**
     * @param Generator<Rows> $batches
     */
    public function __construct(
        private readonly Generator $batches,
    ) {
        $this->load();
    }

    public function current(): Row
    {
        return $this->rows[$this->index];
    }

    public function next(): void
    {
        if (++$this->index >= $this->count) {
            $this->load();
        }
    }

    public function valid(): bool
    {
        return $this->index < $this->count;
    }

    /**
     * Advancing the generator eagerly would decode the next batch while the current one is still
     * being consumed, doubling resident memory per cursor - the generator is only resumed once the
     * current batch is exhausted.
     */
    private function load(): void
    {
        $this->index = 0;

        if ($this->started) {
            $this->batches->next();
        }

        $this->started = true;

        while ($this->batches->valid()) {
            $batch = $this->batches->current();

            if ($batch->count() > 0) {
                $this->rows = array_values($batch->all());
                $this->count = count($this->rows);

                return;
            }

            $this->batches->next();
        }

        $this->rows = [];
        $this->count = 0;
    }
}
