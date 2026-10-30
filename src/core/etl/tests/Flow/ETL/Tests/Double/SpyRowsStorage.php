<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_keys;

final class SpyRowsStorage implements RowsStorage
{
    /**
     * @var array<string, list<Rows>>
     */
    private array $appendedRows = [];

    /**
     * @var array<string, true>
     */
    private array $liveBucketIds = [];

    /**
     * @var array<string, true>
     */
    private array $readBucketIds = [];

    public function __construct(
        private readonly RowsStorage $inner,
    ) {}

    public function append(string $key, Rows $rows): void
    {
        $this->appendedRows[$key][] = $rows;
        $this->liveBucketIds[$key] = true;
        $this->inner->append($key, $rows);
    }

    /**
     * @return array<string, list<Rows>>
     */
    public function appendedRows(): array
    {
        return $this->appendedRows;
    }

    public function close(string $key): void
    {
        $this->inner->close($key);
    }

    public function get(string $key): Generator
    {
        $this->readBucketIds[$key] = true;

        yield from $this->inner->get($key);
    }

    /**
     * @return list<string>
     */
    public function liveBucketIds(): array
    {
        return array_keys($this->liveBucketIds);
    }

    /**
     * @return list<string>
     */
    public function readBucketIds(): array
    {
        return array_keys($this->readBucketIds);
    }

    public function has(string $key): bool
    {
        return $this->inner->has($key);
    }

    public function remove(string $key): void
    {
        unset($this->liveBucketIds[$key]);
        $this->inner->remove($key);
    }

    public function rows(string $key): Cardinality
    {
        return $this->inner->rows($key);
    }

    public function schema(string $key): ?Schema
    {
        return $this->inner->schema($key);
    }
}
