<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinShape;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinShapeTest extends FlowTestCase
{
    public function test_an_unprefixed_join_drops_the_duplicated_column_from_the_right_side(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('name')),
            JoinShape::of(join_on(['id' => 'id']), Join::inner)->schema()->of(
                Join::inner,
                schema(int_schema('id'), str_schema('country')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_a_right_join_drops_the_duplicated_column_from_the_left_side_instead(): void
    {
        static::assertEquals(
            schema(str_schema('country', nullable: true), int_schema('id'), str_schema('name')),
            JoinShape::of(join_on(['id' => 'id']), Join::right)->schema()->of(
                Join::right,
                schema(int_schema('id'), str_schema('country')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_a_left_join_keeps_the_left_column_and_makes_the_right_side_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), str_schema('name', nullable: true)),
            JoinShape::of(join_on(['id' => 'id']), Join::left)->schema()->of(
                Join::left,
                schema(int_schema('id'), str_schema('country')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_a_left_anti_join_answers_the_left_side_alone(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('country')),
            JoinShape::of(join_on(['id' => 'id']), Join::left_anti)->schema()->of(
                Join::left_anti,
                schema(int_schema('id'), str_schema('country')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_a_prefixed_join_keeps_both_columns_and_drops_nothing(): void
    {
        // a prefix already makes the two names distinct, so the duplicate scan does not run
        static::assertEquals(
            schema(int_schema('id'), str_schema('country'), int_schema('right_id'), str_schema('right_name')),
            JoinShape::of(join_on(['id' => 'id'], 'right_'), Join::inner)->schema()->of(
                Join::inner,
                schema(int_schema('id'), str_schema('country')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_join_columns_with_different_names_are_both_kept(): void
    {
        static::assertEquals(
            schema(int_schema('left_id'), str_schema('country'), int_schema('right_id'), str_schema('name')),
            JoinShape::of(join_on(['left_id' => 'right_id']), Join::inner)->schema()->of(
                Join::inner,
                schema(int_schema('left_id'), str_schema('country')),
                schema(int_schema('right_id'), str_schema('name')),
            ),
        );
    }

    public function test_only_the_duplicated_column_is_dropped_when_several_are_compared(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('code'), str_schema('name')),
            JoinShape::of(join_on(['id' => 'id', 'code' => 'other']), Join::inner)->schema()->of(
                Join::inner,
                schema(int_schema('id'), str_schema('code')),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }

    public function test_the_merger_drops_the_same_column_the_schema_does(): void
    {
        static::assertSame(
            ['id' => 1, 'country' => 'PL', 'name' => 'Norbert'],
            JoinShape::of(join_on(['id' => 'id']), Join::inner)
                ->merger()
                ->merge(row(['id' => 1, 'country' => 'PL']), row(['id' => 1, 'name' => 'Norbert']))
                ->toArray(),
        );
    }

    public function test_the_merger_of_a_right_join_keeps_the_right_side_value(): void
    {
        static::assertSame(
            ['country' => 'PL', 'id' => 2, 'name' => 'Norbert'],
            JoinShape::of(join_on(['id' => 'id']), Join::right)
                ->merger()
                ->merge(row(['id' => 1, 'country' => 'PL']), row(['id' => 2, 'name' => 'Norbert']))
                ->toArray(),
        );
    }
}
