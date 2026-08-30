<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class NullRowBuilderTest extends FlowTestCase
{
    public function test_empty_schema_produces_empty_row(): void
    {
        static::assertSame([], (new NullRowBuilder(schema()))->row()->toArray());
    }

    public function test_row_follows_schema_column_order(): void
    {
        static::assertSame(
            ['name', 'id'],
            (new NullRowBuilder(schema(str_schema('name'), int_schema('id'))))->row()->names(),
        );
    }

    public function test_row_nulls_every_schema_column(): void
    {
        static::assertSame(
            ['id' => null, 'name' => null, 'country' => null],
            (new NullRowBuilder(schema(int_schema('id'), str_schema('name'), str_schema('country'))))->row()->toArray(),
        );
    }
}
