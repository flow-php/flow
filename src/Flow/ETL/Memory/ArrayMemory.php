<?php

declare(strict_types=1);

namespace Flow\ETL\Memory;

use Flow\ETL\Exception\InvalidArgumentException;

final class ArrayMemory implements \Countable, Memory
{
    /**
     * @var array<array<string, mixed>>
     */
    public array $data;

    /**
     * @param array<array<string, mixed>> $memory
     */
    public function __construct(array $memory = [])
    {
        $this->assertMemoryStructure($memory);

        $this->data = $memory;
    }

    /**
     * @return array{data: array<array<string, mixed>>}
     */
    public function __serialize() : array
    {
        return [
            'data' => $this->data,
        ];
    }

    /**
     * @param array{data: array<array<string, mixed>>} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->data = $data['data'];
    }

    /**
     * @param array<array<string, mixed>> $data
     */
    public function save(array $data) : void
    {
        $this->assertMemoryStructure($data);

        $this->data = \array_merge($this->data, $data);
    }

    /**
     * Example: [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]].
     *
     * @return array<array<string, mixed>>
     */
    public function dump() : array
    {
        return $this->data;
    }

    /**
     * @param callable(array<string, mixed>) : mixed $callback
     *
     * @return array<mixed>
     */
    public function map(callable $callback) : array
    {
        $data = [];

        foreach ($this->data as $entry) {
            /** @psalm-suppress MixedAssignment */
            $data[] = $callback($entry);
        }

        return $data;
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

        foreach ($this->data as $entry) {
            $data[] = \array_values($entry);
        }

        return \array_merge(...$data);
    }

    /**
     * @param int $size
     *
     * @return array<self>
     */
    public function chunks(int $size) : array
    {
        if ($size < 1) {
            throw InvalidArgumentException::because('Chunk size must be greater than 0');
        }

        $chunks = [];

        foreach (\array_chunk($this->data, $size) as $chunk) {
            $chunks[] = new self($chunk);
        }

        return $chunks;
    }

    public function count() : int
    {
        return \count($this->data);
    }

    /**
     * @param array<array<mixed>> $memory
     *
     * @throws InvalidArgumentException
     */
    private function assertMemoryStructure(array $memory) : void
    {
        foreach ($memory as $entry) {
            /** @psalm-suppress DocblockTypeContradiction */
            if (!\is_array($entry)) {
                throw new InvalidArgumentException('Memory expects nested array data structure: array<array<mixed>>');
            }
        }
    }
}
