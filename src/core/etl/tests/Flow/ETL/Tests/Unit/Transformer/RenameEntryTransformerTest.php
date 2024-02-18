<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\RenameEntryTransformer;
use PHPUnit\Framework\TestCase;

final class RenameEntryTransformerTest extends TestCase
{
    public function test_renaming_entries() : void
    {
        $renameTransformerOne = new RenameEntryTransformer('old_int', 'new_int');
        $renameTransformerTwo = new RenameEntryTransformer('null', 'nothing');

        $rows = $renameTransformerOne->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old_int', 1000),
                    new Row\Entry\IntegerEntry('id', 1),
                    new Row\Entry\StringEntry('status', 'PENDING'),
                    new Row\Entry\BooleanEntry('enabled', true),
                    new Row\Entry\DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\ArrayEntry('array', ['foo', 'bar']),
                    new Row\Entry\JsonEntry('json', ['foo', 'bar']),
                    new Row\Entry\ObjectEntry('object', new \stdClass()),
                    new Row\Entry\NullEntry('null')
                ),
            ),
            $context = new FlowContext(Config::default())
        );

        $rows = $renameTransformerTwo->transform($rows, $context);

        $this->assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('id', 1),
                    new Row\Entry\StringEntry('status', 'PENDING'),
                    new Row\Entry\BooleanEntry('enabled', true),
                    new Row\Entry\DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\ArrayEntry('array', ['foo', 'bar']),
                    new Row\Entry\JsonEntry('json', ['foo', 'bar']),
                    new Row\Entry\ObjectEntry('object', new \stdClass()),
                    new Row\Entry\IntegerEntry('new_int', 1000),
                    new Row\Entry\NullEntry('nothing')
                ),
            ),
            $rows
        );
    }
}
