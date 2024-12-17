<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
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
                    string_entry('status', 'PENDING'),
                    new Row\Entry\BooleanEntry('enabled', true),
                    new Row\Entry\DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\JsonEntry('json', ['foo', 'bar']),
                    string_entry('null', null)
                ),
            ),
            $context = new FlowContext(Config::default())
        );

        $rows = $renameTransformerTwo->transform($rows, $context);

        self::assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('id', 1),
                    string_entry('status', 'PENDING'),
                    new Row\Entry\BooleanEntry('enabled', true),
                    new Row\Entry\DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\JsonEntry('json', ['foo', 'bar']),
                    new Row\Entry\IntegerEntry('new_int', 1000),
                    string_entry('nothing', null)
                ),
            ),
            $rows
        );
    }
}
