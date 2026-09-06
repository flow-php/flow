<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Memory\Memory;

/**
 * Counts dump() calls so a test can assert how many times a memory is read for one run.
 */
final class CountingMemory implements Memory
{
    public int $dumps = 0;

    private readonly ArrayMemory $inner;

    /**
     * @param array<array-key, array<string, mixed>> $memory
     */
    public function __construct(array $memory = [])
    {
        $this->inner = new ArrayMemory($memory);
    }

    public function chunks(int $size): array
    {
        return $this->inner->chunks($size);
    }

    public function count(): int
    {
        return $this->inner->count();
    }

    public function dump(): array
    {
        $this->dumps++;

        return $this->inner->dump();
    }

    public function flatValues(): array
    {
        return $this->inner->flatValues();
    }

    public function map(callable $callback): array
    {
        return $this->inner->map($callback);
    }

    public function save(array $data): void
    {
        $this->inner->save($data);
    }
}
