<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{boolean_entry, flow_context, integer_entry, json_entry, string_entry};
use function Flow\ETL\DSL\{config, row, rows};
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class RenameEntryTransformerTest extends FlowTestCase
{
    public function test_renaming_entries() : void
    {
        $renameTransformerOne = new RenameEntryTransformer('old_int', 'new_int');
        $renameTransformerTwo = new RenameEntryTransformer('null', 'nothing');

        $rows = $renameTransformerOne->transform(
            rows(row(integer_entry('old_int', 1000), integer_entry('id', 1), string_entry('status', 'PENDING'), boolean_entry('enabled', true), new DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')), json_entry('json', ['foo', 'bar']), string_entry('null', null))),
            $context = flow_context(config())
        );

        $rows = $renameTransformerTwo->transform($rows, $context);

        self::assertEquals(
            rows(row(integer_entry('id', 1), string_entry('status', 'PENDING'), boolean_entry('enabled', true), new DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')), json_entry('json', ['foo', 'bar']), integer_entry('new_int', 1000), string_entry('nothing', null))),
            $rows
        );
    }
}
