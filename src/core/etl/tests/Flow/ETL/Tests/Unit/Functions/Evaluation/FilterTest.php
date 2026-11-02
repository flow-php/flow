<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Evaluation;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class FilterTest extends FlowTestCase
{
    public function test_no_predicate_keeps_the_same_rows(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));

        static::assertSame($rows, Filter::bind($rows->schema())->filter($rows, flow_context(config())->backend()));
    }

    public function test_empty_rows_stay_the_same_rows(): void
    {
        $rows = array_to_rows([], schema(int_schema('a')));

        static::assertSame($rows, Filter::bind($rows->schema(), ref('a')->greaterThan(lit(0)))->filter(
            $rows,
            flow_context(config())->backend(),
        ));
    }

    public function test_a_predicate_every_row_matches_keeps_the_same_rows(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));

        static::assertSame($rows, Filter::bind($rows->schema(), ref('a')->greaterThan(lit(0)))->filter(
            $rows,
            flow_context(config())->backend(),
        ));
    }

    public function test_a_predicate_no_row_matches_keeps_no_row(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));

        static::assertSame(
            0,
            Filter::bind($rows->schema(), ref('a')->greaterThan(lit(5)))
                ->filter($rows, flow_context(config())->backend())
                ->count(),
        );
    }

    public function test_a_null_result_drops_the_row_as_false_does(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => null], ['a' => 3]], schema(int_schema('a', nullable: true)));

        static::assertSame(
            [['a' => 3]],
            Filter::bind($rows->schema(), ref('a')->greaterThan(lit(1)))
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_predicate_is_evaluated_only_on_the_rows_the_ones_before_it_kept(): void
    {
        $rows = array_to_rows([['a' => 0], ['a' => 2], ['a' => 10]], schema(int_schema('a')));

        static::assertSame(
            [['a' => 2]],
            Filter::bind($rows->schema(), ref('a')->notEquals(lit(0)), lit(10)->divide(ref('a'))->greaterThan(lit(1)))
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_conjunction_is_evaluated_conjunct_by_conjunct_on_the_rows_the_ones_before_it_kept(): void
    {
        $rows = array_to_rows([['x' => 0], ['x' => 2], ['x' => 10]], schema(int_schema('x')));

        static::assertSame(
            [['x' => 2]],
            Filter::bind(
                $rows->schema(),
                ref('x')->notEquals(lit(0))->and(lit(10)->divide(ref('x'))->greaterThan(lit(1))),
            )
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_conjunct_drops_a_null_row_as_it_drops_a_false_one(): void
    {
        $rows = array_to_rows(
            [['a' => 1, 'b' => 5], ['a' => null, 'b' => 5], ['a' => 3, 'b' => 0], ['a' => 3, 'b' => 5]],
            schema(int_schema('a', nullable: true), int_schema('b')),
        );

        static::assertSame(
            [['a' => 3, 'b' => 5]],
            Filter::bind($rows->schema(), ref('a')->greaterThan(lit(1))->and(ref('b')->greaterThan(lit(1))))
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_row_a_conjunct_leaves_null_is_dropped_before_the_next_conjunct_evaluates_it(): void
    {
        $rows = array_to_rows(
            [['a' => null, 'b' => 0], ['a' => 1, 'b' => 2]],
            schema(int_schema('a', nullable: true), int_schema('b')),
        );

        static::assertSame(
            [['a' => 1, 'b' => 2]],
            Filter::bind(
                $rows->schema(),
                ref('a')->greaterThan(lit(0))->and(lit(10)->divide(ref('b'))->greaterThan(lit(1))),
            )
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_conjunct_returning_no_boolean_inside_a_conjunction_keeps_its_truthy_rows(): void
    {
        $rows = array_to_rows([['a' => 0], ['a' => 2], ['a' => 3]], schema(int_schema('a')));

        static::assertSame(
            [['a' => 2], ['a' => 3]],
            Filter::bind($rows->schema(), ref('a')->greaterThanEqual(lit(0))->and(ref('a')))
                ->filter($rows, flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_a_nested_conjunction_binds_as_its_flat_conjuncts(): void
    {
        $schema = schema(int_schema('a'));
        $a = ref('a')->greaterThan(lit(0));
        $b = ref('a')->lessThan(lit(9));
        $c = ref('a')->notEquals(lit(5));

        static::assertEquals(
            Filter::bind($schema, $a, $b, $c)->describe(),
            Filter::bind($schema, $a->and($b->and($c)))->describe(),
        );
    }

    public function test_a_reference_the_schema_lacks_is_refused(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        Filter::bind(schema(int_schema('a')), ref('missing')->equals(lit(1)));
    }

    public function test_an_expanding_predicate_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'filter() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        Filter::bind(ListColumnsMother::schema(), ref('tags')->expand()->equals(lit('x')));
    }

    public function test_a_predicate_returning_no_boolean_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('filter() requires a predicate returning boolean');

        Filter::bind(schema(int_schema('a')), ref('a'));
    }
}
