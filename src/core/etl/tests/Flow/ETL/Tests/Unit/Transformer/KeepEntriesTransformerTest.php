<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class KeepEntriesTransformerTest extends TestCase
{
    public function test_keeping_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = new KeepEntriesTransformer('name');
        self::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_keeping_not_existing_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = new KeepEntriesTransformer('not_existing');
        self::assertSame(
            [['not_existing' => null]],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
