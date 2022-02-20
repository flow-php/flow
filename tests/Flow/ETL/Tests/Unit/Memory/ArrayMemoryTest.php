<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Memory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayMemoryTest extends TestCase
{
    public function test_map() : void
    {
        $memory = new ArrayMemory([['id' => 1], ['id' => 2]]);

        $this->assertSame([1, 2], $memory->map(fn (array $data) : int => $data['id']));
    }

    public function test_create_memory_from_invalid_data_structure() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Memory expects nested array data structure: array<array<mixed>>');

        new ArrayMemory([1, 2, 3]);
    }

    public function test_save_memory_from_invalid_data_structure() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Memory expects nested array data structure: array<array<mixed>>');

        $memory = new ArrayMemory();
        $memory->save([1, 2, 3]);
    }

    public function test_chunk_size_must_be_greater_than_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk size must be greater than 0');

        (new ArrayMemory())->chunks(0);
    }

    public function test_saving_multiple_entries_into_memory() : void
    {
        $memory = new ArrayMemory();
        $memory->save([['id' => 1], ['id' => 2]]);
        $memory->save([['id' => 3], ['id' => 4]]);

        $this->assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]],
            $memory->dump()
        );
        $this->assertcount(4, $memory);
    }

    public function test_chunks() : void
    {
        $memory = new ArrayMemory([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]);

        $this->assertCount(4, $memory->chunks(1));
        $this->assertCount(1, $memory->chunks(4));
        $this->assertCount(1, $memory->chunks(5));
        $this->assertCount(2, $memory->chunks(2));
    }

    public function test_flat_values() : void
    {
        $memory = new ArrayMemory([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]);

        $this->assertSame([1, 2, 3, 4], $memory->flatValues());
    }
}
