<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, flow_context};
use function Flow\ETL\DSL\{int_entry, json_entry, row, rows, string_entry};
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class DropEntriesTransformerTest extends FlowTestCase
{
    public function test_dropping_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('name', 'Row Name'),
                json_entry('array', ['test'])
            )
        );

        $transformer = new DropEntriesTransformer('id', 'array');
        self::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_removing_not_existing_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('name', 'Row Name'),
                json_entry('array', ['test'])
            )
        );

        $transformer = new DropEntriesTransformer('not_existing');
        self::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray()
        );
    }
}
