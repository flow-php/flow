<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Rows;
use Generator;

use function array_key_exists;
use function array_values;
use function count;
use function usort;

final class Buckets
{
    /**
     * @var array<string, Bucket>
     */
    private array $buckets = [];

    public function __construct(
        private readonly BucketsStorage $storage,
    ) {}

    public function add(Bucket $bucket): void
    {
        $this->buckets[$bucket->id] = $bucket;
    }

    /**
     * @return array<Bucket>
     */
    public function all(): array
    {
        return array_values($this->buckets);
    }

    public function clear(): void
    {
        foreach ($this->buckets as $id => $_) {
            $this->storage->remove($id);
        }

        $this->buckets = [];
    }

    public function get(string $id): Bucket
    {
        if (!array_key_exists($id, $this->buckets)) {
            throw new InvalidArgumentException("Bucket \"{$id}\" does not exist.");
        }

        return $this->buckets[$id];
    }

    public function has(string $id): bool
    {
        return array_key_exists($id, $this->buckets);
    }

    public function remove(string $id): void
    {
        $this->storage->remove($id);
        unset($this->buckets[$id]);
    }

    /**
     * @return Generator<Rows>
     */
    public function rows(string $id, int $batchSize): Generator
    {
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1.');
        }

        $batch = [];

        foreach ($this->storage->get($id) as $row) {
            $batch[] = $row;

            if (count($batch) >= $batchSize) {
                yield new Rows(...$batch);
                $batch = [];
            }
        }

        if ($batch !== []) {
            yield new Rows(...$batch);
        }
    }

    /**
     * @return array<Bucket>
     */
    public function sortByRowsCount(SortOrder $order = SortOrder::ASC): array
    {
        $buckets = $this->all();

        usort($buckets, static fn(Bucket $a, Bucket $b): int => $order === SortOrder::ASC
            ? $a->stats->rowsCount() <=> $b->stats->rowsCount()
            : $b->stats->rowsCount() <=> $a->stats->rowsCount());

        return $buckets;
    }

    public function storage(): BucketsStorage
    {
        return $this->storage;
    }
}
