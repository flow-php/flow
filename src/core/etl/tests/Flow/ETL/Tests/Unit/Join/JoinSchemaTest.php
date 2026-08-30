<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinSchemaTest extends FlowTestCase
{
    public function test_cross_is_the_inner_arm(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            (new JoinSchema())->of(Join::inner, $left, $right),
            (new JoinSchema())->cross($left, $right),
        );
    }

    public function test_dropped_join_columns_are_removed_from_each_side(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('code')),
            (new JoinSchema(dropRight: ['name']))->of(Join::inner, $left, $right),
        );
        static::assertEquals(
            schema(int_schema('id'), str_schema('code'), str_schema('name')),
            (new JoinSchema(dropLeft: ['country']))->of(Join::inner, $left, $right),
        );
    }

    public function test_dropping_a_column_neither_side_declares_is_graceful(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            (new JoinSchema())->of(Join::inner, $left, $right),
            (new JoinSchema(dropLeft: ['nope'], dropRight: ['nope']))->of(Join::inner, $left, $right),
        );
    }

    public function test_inner_keeps_both_sides_non_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('code'), str_schema('name')),
            (new JoinSchema())->of(
                Join::inner,
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_left_anti_keeps_the_left_side_only(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country')),
            (new JoinSchema())->of(
                Join::left_anti,
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_left_makes_the_right_side_nullable(): void
    {
        static::assertEquals(
            schema(
                int_schema('id'),
                str_schema('country'),
                str_schema('code', nullable: true),
                str_schema('name', nullable: true),
            ),
            (new JoinSchema())->of(
                Join::left,
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_prefix_renames_every_right_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('r_code'), str_schema('r_name')),
            (new JoinSchema('r_'))->of(
                Join::inner,
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_right_makes_the_left_side_nullable(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('country', nullable: true),
                str_schema('code'),
                str_schema('name'),
            ),
            (new JoinSchema())->of(
                Join::right,
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }
}
