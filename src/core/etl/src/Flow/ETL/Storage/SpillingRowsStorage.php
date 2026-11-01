<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_key_exists;
use function array_keys;
use function min;

/**
 * Past the memory limit, every frame on $disk holds exactly $batchSize rows.
 */
final class SpillingRowsStorage implements RowsStorage
{
    /**
     * @var array<string, list<Rows>>
     */
    private array $buffers = [];

    /**
     * @var array<string, int>
     */
    private array $buffered = [];

    private readonly MemoryBudget $budget;

    /**
     * @var array<string, true>
     */
    private array $onDisk = [];

    private bool $spilling = false;

    public function __construct(
        public readonly RowsStorage $disk,
        public readonly Unit $memoryLimit,
        private readonly Backend $backend,
        private readonly BatchSize $batchSize = new BatchSize(1000),
    ) {
        $this->budget = new MemoryBudget($this->backend, $this->memoryLimit);
    }

    public function append(string $key, Rows $rows): void
    {
        $this->buffers[$key] ??= [];
        $this->buffered[$key] ??= 0;

        if ($rows->isEmpty()) {
            return;
        }

        $this->buffers[$key][] = $rows;
        $this->buffered[$key] += $rows->count();

        if (!$this->spilling && $this->budget->exceeded()) {
            $this->spilling = true;

            foreach (array_keys($this->buffers) as $id) {
                $this->writeFullBatches($id);
            }

            return;
        }

        if ($this->spilling) {
            $this->writeFullBatches($key);
        }
    }

    public function close(string $key): void
    {
        if (array_key_exists($key, $this->onDisk)) {
            $this->disk->close($key);
        }
    }

    public function get(string $key): Generator
    {
        if (array_key_exists($key, $this->onDisk)) {
            yield from $this->disk->get($key);
        }

        if (($this->buffered[$key] ?? 0) === 0) {
            return;
        }

        $buffered = Rows::concat($this->buffers[$key][0]->schema(), $this->backend, ...$this->buffers[$key]);

        for ($offset = 0, $count = $buffered->count(); $offset < $count; $offset += $this->batchSize->size) {
            yield $buffered->slice($offset, min($this->batchSize->size, $count - $offset));
        }
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->buffers) || array_key_exists($key, $this->onDisk);
    }

    public function remove(string $key): void
    {
        unset($this->buffers[$key], $this->buffered[$key]);

        if (array_key_exists($key, $this->onDisk)) {
            unset($this->onDisk[$key]);
            $this->disk->remove($key);
        }
    }

    public function rows(string $key): Cardinality
    {
        return array_key_exists($key, $this->onDisk)
            ? $this->disk->rows($key)->merge(Cardinality::exact($this->buffered[$key] ?? 0))
            : Cardinality::exact($this->buffered[$key] ?? 0);
    }

    public function schema(string $key): ?Schema
    {
        $schema = array_key_exists($key, $this->onDisk) ? $this->disk->schema($key) : null;

        foreach ($this->buffers[$key] ?? [] as $batch) {
            $schema = ($schema ?? new Schema())->merge($batch->schema());
        }

        return $schema ?? (array_key_exists($key, $this->buffers) ? new Schema() : null);
    }

    public function writeFullBatches(string $key): void
    {
        $count = $this->buffered[$key] ?? 0;

        if ($count < $this->batchSize->size) {
            return;
        }

        $buffered = Rows::concat($this->buffers[$key][0]->schema(), $this->backend, ...$this->buffers[$key]);
        $written = $count - ($count % $this->batchSize->size);

        for ($offset = 0; $offset < $written; $offset += $this->batchSize->size) {
            $this->disk->append($key, $buffered->slice($offset, $this->batchSize->size));
        }

        $this->onDisk[$key] = true;
        $this->budget->released();

        if ($written === $count) {
            unset($this->buffers[$key], $this->buffered[$key]);

            return;
        }

        $this->buffers[$key] = [$buffered->slice($written, $count - $written)];
        $this->buffered[$key] = $count - $written;
    }
}
