<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{boolean_entry, config, flow_context, integer_entry, json_entry, row, rows, string_entry};
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\RenameEntryTransformer;

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

    public function test_renaming_entry_preserves_metadata() : void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = string_entry('old_name', 'test value', $metadata);
        $inputRows = rows(row($entry));

        $transformer = new RenameEntryTransformer('old_name', 'new_name');
        $outputRows = $transformer->transform($inputRows, flow_context(config()));

        $renamedEntry = $outputRows->first()->get('new_name');

        self::assertSame('new_name', $renamedEntry->name());
        self::assertSame('test value', $renamedEntry->value());
        self::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renaming_to_same_name_returns_same_rows_instance() : void
    {
        $inputRows = rows(row(string_entry('name', 'value')));

        $transformer = new RenameEntryTransformer('name', 'name');
        $outputRows = $transformer->transform($inputRows, flow_context(config()));

        self::assertSame($inputRows, $outputRows);
    }
}
