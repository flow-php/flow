<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class MemoryExtractorTest extends TestCase
{
    public function chunk_sizes() : \Generator
    {
        yield [1];
        yield [2];
        yield [3];
        yield [4];
    }

    public function test_chunk_size_must_be_greater_than_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk size must be greater than 0');

        From::memory(new ArrayMemory(), 0);
    }

    /**
     * @dataProvider chunk_sizes
     */
    public function test_memory_extractor(int $chunkSize) : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('number', 1), Entry::string('name', 'one')),
            Row::create(Entry::integer('number', 2), Entry::string('name', 'two')),
            Row::create(Entry::integer('number', 3), Entry::string('name', 'tree')),
            Row::create(Entry::integer('number', 4), Entry::string('name', 'four')),
            Row::create(Entry::integer('number', 5), Entry::string('name', 'five')),
        );

        $memory = new ArrayMemory();

        (To::memory($memory))->load($rows);

        $extractor = From::memory($memory, $chunkSize);

        $data = [];

        foreach ($extractor->extract() as $rowsData) {
            $data  = \array_merge($data, $rowsData->toArray());
        }

        $this->assertSame(
            [
                ['row' => ['number' => 1, 'name' => 'one']],
                ['row' => ['number' => 2, 'name' => 'two']],
                ['row' => ['number' => 3, 'name' => 'tree']],
                ['row' => ['number' => 4, 'name' => 'four']],
                ['row' => ['number' => 5, 'name' => 'five']],
            ],
            $data
        );
    }
}
