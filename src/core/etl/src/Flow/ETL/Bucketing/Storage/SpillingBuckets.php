<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing\Storage;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\ResidentBucketsStorage;
use Flow\ETL\Column\Backend;
use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\MemoryBudget;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Generator;

use function array_key_exists;
use function array_keys;
use function array_slice;
use function min;

/**
 * Keeps buckets in memory while the process stays under the memory limit. The first time it passes the limit every
 * bucket moves its full batches to $disk; from then on a bucket writes to $disk each time it holds a full batch, so
 * every frame on $disk holds exactly $batchSize rows and memory stays bounded by one batch per bucket. The rest of a
 * bucket stays buffered and is read after its frames.
 */
final class SpillingBuckets implements BucketsStorage
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

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public readonly BucketsStorage $disk,
        public readonly Unit $memoryLimit,
        private readonly Backend $backend,
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }

        $this->budget = new MemoryBudget($this->backend, $this->memoryLimit);
    }

    /**
     * $bucketing's storage behind the memory limit - a resident storage already holds everything in memory.
     */
    public static function around(BucketingConfig $bucketing, Unit $memoryLimit, Backend $backend): BucketsStorage
    {
        return $bucketing->storage instanceof ResidentBucketsStorage
            ? $bucketing->storage
            : new self($bucketing->storage, $memoryLimit, $backend, $bucketing->batchSize);
    }

    public function append(string $bucketId, Rows $rows): void
    {
        if ($rows->isEmpty()) {
            return;
        }

        $this->buffers[$bucketId][] = $rows;
        $this->buffered[$bucketId] = ($this->buffered[$bucketId] ?? 0) + $rows->count();

        if (!$this->spilling && $this->budget->exceeded()) {
            $this->spilling = true;

            foreach (array_keys($this->buffers) as $id) {
                $this->writeFullBatches($id);
            }

            return;
        }

        if ($this->spilling) {
            $this->writeFullBatches($bucketId);
        }
    }

    public function get(string $bucketId): Generator
    {
        if (array_key_exists($bucketId, $this->onDisk)) {
            yield from $this->disk->get($bucketId);
        }

        if (!array_key_exists($bucketId, $this->buffers)) {
            return;
        }

        $buffered = $this->buffers[$bucketId][0]->concat($this->backend, ...array_slice($this->buffers[$bucketId], 1));

        for ($offset = 0, $count = $buffered->count(); $offset < $count; $offset += $this->batchSize) {
            yield $buffered->slice($offset, min($this->batchSize, $count - $offset));
        }
    }

    public function remove(string $bucketId): void
    {
        unset($this->buffers[$bucketId], $this->buffered[$bucketId]);

        if (array_key_exists($bucketId, $this->onDisk)) {
            unset($this->onDisk[$bucketId]);
            $this->disk->remove($bucketId);
        }
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->remove($bucketId);
        $this->append($bucketId, $rows);
    }

    public function writeFullBatches(string $bucketId): void
    {
        $count = $this->buffered[$bucketId] ?? 0;

        if ($count < $this->batchSize) {
            return;
        }

        $buffered = $this->buffers[$bucketId][0]->concat($this->backend, ...array_slice($this->buffers[$bucketId], 1));
        $written = $count - ($count % $this->batchSize);

        for ($offset = 0; $offset < $written; $offset += $this->batchSize) {
            $this->disk->append($bucketId, $buffered->slice($offset, $this->batchSize));
        }

        $this->onDisk[$bucketId] = true;
        $this->budget->released();

        if ($written === $count) {
            unset($this->buffers[$bucketId], $this->buffered[$bucketId]);

            return;
        }

        $this->buffers[$bucketId] = [$buffered->slice($written, $count - $written)];
        $this->buffered[$bucketId] = $count - $written;
    }
}
