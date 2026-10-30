<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Memory;

use Countable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function array_chunk;
use function array_merge;
use function array_values;
use function count;
use function is_array;

final class ArrayMemory implements Countable, Memory
{
    /**
     * @param array<array-key, array<string, mixed>> $memory
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private array $memory = [],
    ) {
        $this->assertMemoryStructure($memory);
    }

    /**
     * @return array<self>
     */
    public function chunks(int $size): array
    {
        if ($size < 1) {
            throw InvalidArgumentException::because('Chunk size must be greater than 0');
        }

        $chunks = [];

        foreach (array_chunk($this->memory, $size) as $chunk) {
            $chunks[] = new self($chunk);
        }

        return $chunks;
    }

    public function count(): int
    {
        return count($this->memory);
    }

    /**
     * Example: [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]].
     *
     * @return array<array<string, mixed>>
     */
    public function dump(): array
    {
        return $this->memory;
    }

    /**
     * This method is a combination of array_map and array_values functions.
     *
     * Turns: [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]
     * Into: [1, 2, 3, 4]
     *
     * @return array<mixed>
     */
    public function flatValues(): array
    {
        $data = [];

        foreach ($this->memory as $row) {
            $data[] = array_values($row);
        }

        return array_merge(...$data);
    }

    /**
     * @param callable(array<string, mixed>) : mixed $callback
     *
     * @return array<mixed>
     */
    public function map(callable $callback): array
    {
        $data = [];

        foreach ($this->memory as $row) {
            $data[] = $callback($row);
        }

        return $data;
    }

    /**
     * @param array<array<string, mixed>> $data
     */
    public function save(array $data): void
    {
        $this->assertMemoryStructure($data);

        foreach ($data as $row) {
            $this->memory[] = $row;
        }
    }

    /**
     * @param array<mixed> $memory
     *
     * @throws InvalidArgumentException
     */
    private function assertMemoryStructure(array $memory): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($memory as $row) {
            if (!is_array($row)) {
                throw new InvalidArgumentException('Memory expects nested array data structure: array<array<mixed>>');
            }
        }
    }
}
