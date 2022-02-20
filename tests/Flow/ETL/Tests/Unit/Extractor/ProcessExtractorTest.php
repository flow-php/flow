<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ProcessExtractorTest extends TestCase
{
    public function test_process_extractor() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'tree')),
            Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
            Row::create(new IntegerEntry('number', 5), new StringEntry('name', 'five')),
        );

        $extractor = new ProcessExtractor($rows);

        $data = [];

        foreach ($extractor->extract() as $rowsData) {
            $data  = \array_merge($data, $rowsData->toArray());
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
