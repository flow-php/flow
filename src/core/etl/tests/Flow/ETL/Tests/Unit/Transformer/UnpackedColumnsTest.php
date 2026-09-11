<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UnpackedColumns;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class UnpackedColumnsTest extends FlowTestCase
{
    public function test_a_declared_column_that_collides_replaces_rather_than_duplicates(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('p.colour', nullable: true)),
            (new UnpackedColumns())->of(
                schema(int_schema('id'), int_schema('p.colour')),
                'p.',
                schema(str_schema('colour')),
            ),
        );
    }

    public function test_every_declared_column_is_added_nullable_and_prefixed(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('p.colour', nullable: true), bool_schema('p.boxed', nullable: true)),
            (new UnpackedColumns())->of(
                schema(int_schema('id')),
                'p.',
                schema(str_schema('colour'), bool_schema('boxed')),
            ),
        );
    }

    public function test_the_declaration_order_is_the_column_order(): void
    {
        static::assertSame(
            ['id', 'p.b', 'p.a'],
            (new UnpackedColumns())
                ->of(schema(int_schema('id')), 'p.', schema(str_schema('b'), str_schema('a')))
                ->references()
                ->names(),
        );
    }
}
