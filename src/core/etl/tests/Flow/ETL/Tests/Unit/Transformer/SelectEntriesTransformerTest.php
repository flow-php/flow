<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;

final class SelectEntriesTransformerTest extends FlowTestCase
{
    public function test_selecting_entries(): void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'Row Name'), json_entry('array', ['test'])));

        $transformer = new SelectEntriesTransformer('name');
        static::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }

    public function test_selecting_not_existing_entries(): void
    {
        $rows = rows(row(int_entry('id', 1), string_entry('name', 'Row Name'), json_entry('array', ['test'])));

        $transformer = new SelectEntriesTransformer('not_existing');
        static::assertSame(
            [['not_existing' => null]],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }

    public function test_using_select_entries_in_order_to_change_entries_order(): void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'Row Name'), json_entry('array', ['test'])));

        $transformer = new SelectEntriesTransformer('name', 'id', 'array');
        static::assertSame(
            [
                ['name' => 'Row Name', 'id' => 1, 'array' => ['test']],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }
}
