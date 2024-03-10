<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
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
        self::assertSame(
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
        self::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
