<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Records every bucket id each operation was asked for, so a test can assert which storage a run was
 * routed to. BucketsStorage::get() is silent for an unknown id in every implementation, so a routing
 * miss reads zero rows rather than throwing - recording the calls is the only way to see it.
 */
final class RecordingRowsStorage implements RowsStorage
{
    /**
     * @var list<string>
     */
    public array $appended = [];

    /**
     * @var list<string>
     */
    public array $read = [];

    /**
     * @var array<string, list<int>> the row count of every append, by bucket; the rows themselves are not kept
     */
    public array $rowCounts = [];

    public function __construct(
        private readonly RowsStorage $inner,
    ) {}

    public function append(string $key, Rows $rows): void
    {
        $this->appended[] = $key;
        $this->rowCounts[$key][] = $rows->count();
        $this->inner->append($key, $rows);
    }

    public function close(string $key): void
    {
        $this->inner->close($key);
    }

    public function get(string $key): Generator
    {
        $this->read[] = $key;

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
