<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropEntriesTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;

final class DropEntriesTransformerTest extends FlowTestCase
{
    public function test_dropping_entries(): void
    {
        $rows = rows(
            schema(int_schema('id'), string_schema('name'), json_schema('array')),
            row(['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]),
        );

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
        $rows = rows(
            schema(int_schema('id'), string_schema('name'), json_schema('array')),
            row(['id' => 1, 'name' => 'Row Name', 'array' => type_json()->cast(['test'])]),
        );

        $transformer = new DropEntriesTransformer('not_existing');
        static::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray(),
        );
    }
}
