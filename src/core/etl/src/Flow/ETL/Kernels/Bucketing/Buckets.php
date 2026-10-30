<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Rows;
use Generator;

use function array_values;

final class Buckets
{
    /**
     * @var array<string, Bucket>
     */
    private array $buckets = [];

    public function __construct(
        private readonly RowsStorage $storage,
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

    public function remove(string $id): void
    {
        $this->storage->remove($id);
        unset($this->buckets[$id]);
    }

    /**
     * @return Generator<Rows>
     */
    public function rows(string $id): Generator
    {
        yield from $this->storage->get($id);
    }

    public function storage(): RowsStorage
    {
        return $this->storage;
    }
}
