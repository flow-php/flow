<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\from_rows;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ProcessExtractorTest extends TestCase
{
    public function test_process_extractor() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('number', 1), Entry::string('name', 'one')),
            Row::create(Entry::integer('number', 2), Entry::string('name', 'two')),
            Row::create(Entry::integer('number', 3), Entry::string('name', 'tree')),
            Row::create(Entry::integer('number', 4), Entry::string('name', 'four')),
            Row::create(Entry::integer('number', 5), Entry::string('name', 'five')),
        );

        $extractor = from_rows($rows);

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
            $rows->toArray()
        );
    }
}
