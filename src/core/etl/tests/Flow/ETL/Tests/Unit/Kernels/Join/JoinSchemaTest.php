<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Join;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Kernels\Join\JoinSchema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Join\JoinType;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinSchemaTest extends FlowTestCase
{
    public function test_cross_is_the_inner_arm(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            (new JoinSchema(type: JoinType::inner))->output($left, $right),
            (new JoinSchema())->cross($left, $right),
        );
    }

    public function test_dropped_join_columns_are_removed_from_each_side(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('code')),
            (new JoinSchema(dropRight: ['name'], type: JoinType::inner))->output($left, $right),
        );
        static::assertEquals(
            schema(int_schema('id'), str_schema('code'), str_schema('name')),
            (new JoinSchema(dropLeft: ['country'], type: JoinType::inner))->output($left, $right),
        );
    }

    public function test_dropping_a_column_neither_side_declares_is_graceful(): void
    {
        $left = schema(int_schema('id'), str_schema('country'));
        $right = schema(str_schema('code'), str_schema('name'));

        static::assertEquals(
            (new JoinSchema(type: JoinType::inner))->output($left, $right),
            (new JoinSchema(dropLeft: ['nope'], dropRight: ['nope'], type: JoinType::inner))->output($left, $right),
        );
    }

    public function test_inner_keeps_both_sides_non_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('code'), str_schema('name')),
            (new JoinSchema(type: JoinType::inner))->output(
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_left_anti_keeps_the_left_side_only(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country')),
            (new JoinSchema(type: JoinType::left_anti))->output(
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
            (new JoinSchema(type: JoinType::left))->output(
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_prefix_renames_every_right_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('r_code'), str_schema('r_name')),
            (new JoinSchema('r_', type: JoinType::inner))->output(
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
            (new JoinSchema(type: JoinType::right))->output(
                schema(int_schema('id'), str_schema('country')),
                schema(str_schema('code'), str_schema('name')),
            ),
        );
    }

    public function test_a_cross_name_collision_suggests_a_prefix(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessageMatches(
            '/Column definitions must be unique.*\. Please consider using join prefix option$/',
        );

        (new JoinSchema())->cross(schema(int_schema('id')), schema(int_schema('id')));
    }

    public function test_a_keyed_join_name_collision_suggests_a_prefix(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessageMatches(
            '/Column definitions must be unique.*\. Please consider using join prefix option$/',
        );

        (new JoinSchema(type: JoinType::left))->output(
            schema(int_schema('id'), str_schema('name')),
            schema(str_schema('name')),
        );
    }

    public function test_an_unprefixed_join_drops_the_duplicated_column_from_the_right_side(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('name')),
            join_on(['id' => 'id'])
                ->schema(JoinType::inner)
                ->output(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_a_right_join_drops_the_duplicated_column_from_the_left_side_instead(): void
    {
        static::assertEquals(
            schema(str_schema('country', nullable: true), int_schema('id'), str_schema('name')),
            join_on(['id' => 'id'])
                ->schema(JoinType::right)
                ->output(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_a_left_join_keeps_the_left_column_and_makes_the_right_side_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('name', nullable: true)),
            join_on(['id' => 'id'])
                ->schema(JoinType::left)
                ->output(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_a_left_anti_join_answers_the_left_side_alone(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country')),
            join_on(['id' => 'id'])
                ->schema(JoinType::left_anti)
                ->output(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_a_prefixed_join_keeps_both_columns_and_drops_nothing(): void
    {
        // a prefix already makes the two names distinct, so the duplicate scan does not run
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), int_schema('right_id'), str_schema('right_name')),
            join_on(['id' => 'id'], 'right_')
                ->schema(JoinType::inner)
                ->output(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_join_columns_with_different_names_are_both_kept(): void
    {
        static::assertEquals(
            schema(int_schema('left_id'), str_schema('country'), int_schema('right_id'), str_schema('name')),
            join_on(['left_id' => 'right_id'])
                ->schema(JoinType::inner)
                ->output(
                    schema(int_schema('left_id'), str_schema('country')),
                    schema(int_schema('right_id'), str_schema('name')),
                ),
        );
    }

    public function test_only_the_duplicated_column_is_dropped_when_several_are_compared(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('code'), str_schema('name')),
            join_on(['id' => 'id', 'code' => 'other'])
                ->schema(JoinType::inner)
                ->output(schema(int_schema('id'), str_schema('code')), schema(int_schema('id'), str_schema('name'))),
        );
    }

    public function test_the_merger_drops_the_same_column_the_schema_does(): void
    {
        static::assertSame(
            ['id' => 1, 'country' => 'PL', 'name' => 'Norbert'],
            join_on(['id' => 'id'])
                ->schema(JoinType::inner)
                ->merger(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name')))
                ->merge(
                    array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
                    array_to_rows([['id' => 1, 'name' => 'Norbert']], schema(int_schema('id'), str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_the_merger_of_a_right_join_keeps_the_right_side_value(): void
    {
        static::assertSame(
            ['country' => 'PL', 'id' => 2, 'name' => 'Norbert'],
            join_on(['id' => 'id'])
                ->schema(JoinType::right)
                ->merger(schema(int_schema('id'), str_schema('country')), schema(int_schema('id'), str_schema('name')))
                ->merge(
                    array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
                    array_to_rows([['id' => 2, 'name' => 'Norbert']], schema(int_schema('id'), str_schema('name'))),
                )
                ->values(0),
        );
    }
}
