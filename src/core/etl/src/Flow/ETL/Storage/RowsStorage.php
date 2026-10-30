<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

interface RowsStorage
{
    /**
     * An empty batch still makes has() answer true for the key.
     */
    public function append(string $key, Rows $rows): void;

    /**
     * Finishes the key's pending writes so another process can read it; a no-op for a key with none.
     */
    public function close(string $key): void;

    /**
     * Yields the key's rows back in batches - batch shape is storage-defined, consumers must not
     * assume any particular batch size.
     *
     * @return Generator<Rows>
     */
    public function get(string $key): Generator;

    public function has(string $key): bool;

    public function remove(string $key): void;

    public function rows(string $key): Cardinality;

    /**
     * The schema every batch of the key fits; null for a missing key.
     */
    public function schema(string $key): ?Schema;
}
