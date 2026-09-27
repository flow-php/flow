<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter;

use Flow\ETL\Schema\Formatter\InlineSchemaFormatter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class InlineSchemaFormatterTest extends FlowTestCase
{
    public function test_formatting_an_empty_schema(): void
    {
        static::assertSame('', (new InlineSchemaFormatter())->format(schema()));
    }

    public function test_formatting_names_nullability_and_types_in_schema_order(): void
    {
        static::assertSame(
            'id: integer, name: ?string, tags: list<integer>',
            (new InlineSchemaFormatter())->format(schema(
                int_schema('id'),
                str_schema('name', nullable: true),
                list_schema('tags', type_list(type_integer())),
            )),
        );
    }
}
