<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class MemoryExtractorTest extends TestCase
{
    public function test_memory_extractor() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('number', 1), Entry::string('name', 'one')),
            Row::create(Entry::integer('number', 2), Entry::string('name', 'two')),
            Row::create(Entry::integer('number', 3), Entry::string('name', 'tree')),
            Row::create(Entry::integer('number', 4), Entry::string('name', 'four')),
            Row::create(Entry::integer('number', 5), Entry::string('name', 'five')),
        );

        $memory = new ArrayMemory();

        (to_memory($memory))->load($rows, new FlowContext(Config::default()));

        $extractor = from_memory($memory);

        $data = [];

        foreach ($extractor->extract(new FlowContext(Config::default())) as $rowsData) {
            $data = [...$data, ...$rowsData->toArray()];
        }

        $this->assertSame(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'tree'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            $data
        );
    }
}
