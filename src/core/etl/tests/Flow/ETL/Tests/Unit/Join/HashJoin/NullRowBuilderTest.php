<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class NullRowBuilderTest extends FlowTestCase
{
    public function test_empty_schema_produces_one_row_without_columns(): void
    {
        $rows = (new NullRowBuilder(schema(), new AdaptiveBackend()))->rows();

        static::assertSame(1, $rows->count());
        static::assertSame([], $rows->columns());
    }

    public function test_row_follows_schema_column_order(): void
    {
        static::assertSame(
            ['name', 'id'],
            array_keys(
                (new NullRowBuilder(schema(str_schema('name'), int_schema('id')), new AdaptiveBackend()))
                    ->rows()
                    ->columns(),
            ),
        );
    }

    public function test_row_nulls_every_schema_column_under_a_nullable_schema(): void
    {
        $rows = (new NullRowBuilder(
            schema(int_schema('id'), str_schema('name'), str_schema('country')),
            new AdaptiveBackend(),
        ))->rows();

        static::assertSame([['id' => null, 'name' => null, 'country' => null]], $rows->toArray());
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('country', nullable: true),
            ),
            $rows->schema(),
        );
    }
}
