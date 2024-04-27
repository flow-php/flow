<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{array_entry, int_entry, row, rows, str_entry, string_entry};
use Flow\ETL\Transformer\SelectEntriesTransformer;
use Flow\ETL\{Config, FlowContext};
use PHPUnit\Framework\TestCase;

final class SelectEntriesTransformerTest extends TestCase
{
    public function test_selecting_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                str_entry('name', 'Row Name'),
                array_entry('array', ['test'])
            )
        );

        $transformer = new SelectEntriesTransformer('name');
        self::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_selecting_not_existing_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('name', 'Row Name'),
                array_entry('array', ['test'])
            )
        );

        $transformer = new SelectEntriesTransformer('not_existing');
        self::assertSame(
            [['not_existing' => null]],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_using_select_entries_in_order_to_change_entries_order() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                str_entry('name', 'Row Name'),
                array_entry('array', ['test'])
            )
        );

        $transformer = new SelectEntriesTransformer('name', 'id', 'array');
        self::assertSame(
            [
                ['name' => 'Row Name', 'id' => 1, 'array' => ['test']],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
