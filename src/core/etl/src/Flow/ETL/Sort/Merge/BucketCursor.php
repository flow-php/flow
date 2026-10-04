<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Sort\RowOrder;
use Flow\ETL\Sort\SortKey;
use Generator;

final class BucketCursor
{
    private Rows $batch;

    private int $index = 0;

    /**
     * @var list<SortKey> $batch's sort keys
     */
    private array $keys = [];

    private ?Schema $schema = null;

    private bool $started = false;

    /**
     * @param Generator<Rows> $batches
     */
    public function __construct(
        private readonly Generator $batches,
        private readonly RowOrder $order,
        private readonly Backend $backend,
    ) {
        $this->batch = Rows::empty(new Schema(), $this->backend);
        $this->load();
    }

    public function advance(int $index): void
    {
        $this->index = $index;

        if ($this->index >= $this->batch->count()) {
            $this->load();
        }
    }

    public function batch(): Rows
    {
        return $this->batch;
    }

    public function index(): int
    {
        return $this->index;
    }

    /**
     * @return list<SortKey>
     */
    public function keys(): array
    {
        return $this->keys;
    }

    public function schema(): Schema
    {
        return $this->schema ?? new Schema();
    }

    public function valid(): bool
    {
        return $this->index < $this->batch->count();
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
                $this->schema ??= $batch->schema();
                $this->batch = $batch;
                $this->keys = $this->order->keys($batch);

                return;
            }

            $this->batches->next();
        }

        $this->batch = Rows::empty(new Schema(), $this->backend);
        $this->keys = [];
    }
}
