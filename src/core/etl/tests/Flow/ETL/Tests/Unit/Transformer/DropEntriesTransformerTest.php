<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropEntriesTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

final class DropEntriesTransformerTest extends FlowTestCase
{
    public function test_dropping_entries(): void
    {
        $rows = rows(row(int_entry('id', 1), string_entry('name', 'Row Name'), json_entry('array', ['test'])));

        $transformer = new DropEntriesTransformer('id', 'array');
        static::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }

    public function test_removing_not_existing_entries(): void
    {
        $rows = rows(row(int_entry('id', 1), string_entry('name', 'Row Name'), json_entry('array', ['test'])));

        $transformer = new DropEntriesTransformer('not_existing');
        static::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }
}
