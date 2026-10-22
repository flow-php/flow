<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Evaluation;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Function\Evaluation\Filter;
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

        static::assertSame($rows, Filter::bind($rows->schema())->filter($rows, flow_context(config())));
    }

    public function test_empty_rows_stay_the_same_rows(): void
    {
        $rows = array_to_rows([], schema(int_schema('a')));

        static::assertSame($rows, Filter::bind($rows->schema(), ref('a')->greaterThan(lit(0)))->filter(
            $rows,
            flow_context(config()),
        ));
    }

    public function test_a_predicate_every_row_matches_keeps_the_same_rows(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));

        static::assertSame($rows, Filter::bind($rows->schema(), ref('a')->greaterThan(lit(0)))->filter(
            $rows,
            flow_context(config()),
        ));
    }

    public function test_a_predicate_no_row_matches_keeps_no_row(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));

        static::assertSame(
            0,
            Filter::bind($rows->schema(), ref('a')->greaterThan(lit(5)))
                ->filter($rows, flow_context(config()))
                ->count(),
        );
    }

    public function test_a_null_result_drops_the_row_as_false_does(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => null], ['a' => 3]], schema(int_schema('a', nullable: true)));

        static::assertSame(
            [['a' => 3]],
            Filter::bind($rows->schema(), ref('a')->greaterThan(lit(1)))
                ->filter($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_a_predicate_is_evaluated_only_on_the_rows_the_ones_before_it_kept(): void
    {
        $rows = array_to_rows([['a' => 0], ['a' => 2], ['a' => 10]], schema(int_schema('a')));

        static::assertSame(
            [['a' => 2]],
            Filter::bind($rows->schema(), ref('a')->notEquals(lit(0)), lit(10)->divide(ref('a'))->greaterThan(lit(1)))
                ->filter($rows, flow_context(config()))
                ->toArray(),
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
