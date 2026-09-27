<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\DeclaredColumns;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DeclaredColumnsTest extends FlowTestCase
{
    public function test_keeps_every_declared_column(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'one'], ['id' => 2]],
            (new DeclaredColumns())->project(
                [['id' => 1, 'name' => 'one'], ['id' => 2]],
                schema(int_schema('id'), str_schema('name', nullable: true)),
            ),
        );
    }

    public function test_drops_a_column_the_schema_does_not_declare(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new DeclaredColumns())->project([
                ['id' => 1, 'name' => 'one'],
                ['id' => 2, 'nmae' => 'two'],
            ], schema(int_schema('id'))),
        );
    }

    public function test_keeps_a_numeric_column_name_declared_as_is(): void
    {
        static::assertSame(
            [[2024 => 1]],
            (new DeclaredColumns())->project([['2024' => 1, 'other' => 2]], schema(int_schema('2024'))),
        );
    }

    public function test_keeps_a_position_declared_by_its_positional_name(): void
    {
        static::assertSame([[0 => 'a']], (new DeclaredColumns())->project([['a', 'b']], schema(str_schema('e00'))));
    }

    public function test_an_empty_batch_projects_to_nothing(): void
    {
        static::assertSame([], (new DeclaredColumns())->project([], schema(int_schema('id'))));
    }
}
