<?php

declare(strict_types=1);

namespace Flow\ETL\Memory;

use Flow\ETL\Exception\InvalidArgumentException;

final class ArrayMemory implements \Countable, Memory
{
    /**
     * @param array<array-key, array<string, mixed>> $memory
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private array $memory = [])
    {
        $this->assertMemoryStructure($memory);
    }

    /**
     * @return array<self>
     */
    public function chunks(int $size) : array
    {
        if ($size < 1) {
            throw InvalidArgumentException::because('Chunk size must be greater than 0');
        }

        $chunks = [];

        foreach (\array_chunk($this->memory, $size) as $chunk) {
            $chunks[] = new self($chunk);
        }

        return $chunks;
    }

    public function count() : int
    {
        return \count($this->memory);
    }

    /**
     * Example: [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]].
     *
     * @return array<array<string, mixed>>
     */
    public function dump() : array
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
    public function flatValues() : array
    {
        $data = [];

        foreach ($this->memory as $entry) {
            $data[] = \array_values($entry);
        }

        return \array_merge(...$data);
    }

    /**
     * @param callable(array<string, mixed>) : mixed $callback
     *
     * @return array<mixed>
     */
    public function map(callable $callback) : array
    {
        $data = [];

        foreach ($this->memory as $entry) {
            $data[] = $callback($entry);
        }

        return $data;
    }

    /**
     * @param array<array<string, mixed>> $data
     */
    public function save(array $data) : void
    {
        $this->assertMemoryStructure($data);

        $this->memory = \array_merge($this->memory, $data);
    }

    /**
     * @param array<mixed> $memory
     *
     * @throws InvalidArgumentException
     */
    private function assertMemoryStructure(array $memory) : void
    {
        foreach ($memory as $entry) {
            if (!\is_array($entry)) {
                throw new InvalidArgumentException('Memory expects nested array data structure: array<array<mixed>>');
            }
        }
    }
}
