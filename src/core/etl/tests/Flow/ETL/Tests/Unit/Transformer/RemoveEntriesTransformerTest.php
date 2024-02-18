<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use PHPUnit\Framework\TestCase;

final class RemoveEntriesTransformerTest extends TestCase
{
    public function test_removing_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = new RemoveEntriesTransformer('id', 'array');
        $this->assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_removing_not_existing_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = new RemoveEntriesTransformer('not_existing');
        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
