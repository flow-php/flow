<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Records the id of every append(), delegating the storage to MemoryBuckets.
 */
final class IdRecordingRowsStorage implements RowsStorage
{
    /**
     * @var list<string>
     */
    public array $ids = [];

    private readonly MemoryRowsStorage $inner;

    public function __construct()
    {
        $this->inner = new MemoryRowsStorage();
    }

    public function append(string $key, Rows $rows): void
    {
        $this->ids[] = $key;
        $this->inner->append($key, $rows);
    }

    public function close(string $key): void
    {
        $this->inner->close($key);
    }

    public function get(string $key): Generator
    {
        yield from $this->inner->get($key);
    }

    public function has(string $key): bool
    {
        return $this->inner->has($key);
    }

    public function remove(string $key): void
    {
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
