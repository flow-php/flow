<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ScalarFunctionFilterTransformerTest extends FlowTestCase
{
    public function test_bind_refuses_a_non_boolean_predicate(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new ScalarFunctionFilterTransformer(ref('a')))->bind(schema(int_schema('a')));
    }

    public function test_bind_refuses_a_predicate_referencing_a_column_missing_from_the_input(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new ScalarFunctionFilterTransformer(ref('missing')->equals(lit(1))))->bind(schema(int_schema('a')));
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('a'), int_schema('b'));

        static::assertEquals(
            $input,
            (new ScalarFunctionFilterTransformer(ref('a')->equals(ref('b'))))->bind($input)->output,
        );
    }

    public function test_equal(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->equals(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_equal_on_literal(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->equals(lit(2))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_greater_than(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThan(ref('a'))))
                ->transform(
                    array_to_rows([['a' => 1, 'b' => 2]], schema(int_schema('a'), int_schema('b'))),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_greater_than_or_equal(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThanEqual(ref('a'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_less_than(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThan(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_less_than_equal(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThanEqual(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_not_equal(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notEquals(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_not_same(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notSame(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_same(): void
    {
        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->same(ref('b'))))
                ->transform(
                    array_to_rows(
                        [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
                        schema(int_schema('a'), int_schema('b')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_a_non_boolean_predicate_is_rejected_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('filter() requires a predicate returning boolean');

        (new ScalarFunctionFilterTransformer(ref('score')))->transform(array_to_rows([
            ['score' => 1],
            ['score' => null],
        ], schema(int_schema('score', nullable: true))), flow_context(config())->backend());
    }

    public function test_a_null_propagating_predicate_over_a_nullable_column_is_accepted(): void
    {
        static::assertSame(
            [
                ['score' => 11],
            ],
            (new ScalarFunctionFilterTransformer(ref('score')->greaterThan(lit(10))))
                ->transform(array_to_rows([
                    ['score' => 11],
                    ['score' => 2],
                    ['score' => null],
                ], schema(int_schema('score', nullable: true))), flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_an_empty_batch_is_bound_against_its_own_schema(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new ScalarFunctionFilterTransformer(ref('missing')->equals(lit(1))))->transform(
            rows(schema()),
            flow_context(config())->backend(),
        );
    }

    public function test_bind_refuses_an_expand_in_the_predicate(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'filter() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        (new ScalarFunctionFilterTransformer(ref('tags')->expand()->equals(lit('x'))))->bind(
            ListColumnsMother::schema(),
        );
    }

    public function test_bind_refuses_a_root_expand_over_booleans(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'filter() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        (new ScalarFunctionFilterTransformer(ref('flags')->expand()))->bind(ListColumnsMother::schema());
    }

    public function test_the_bound_transformer_keeps_the_rows_its_predicate_matches(): void
    {
        $input = schema(int_schema('a'));
        $bound = (new ScalarFunctionFilterTransformer(ref('a')->greaterThan(lit(1))))->bind($input)->step;

        static::assertInstanceOf(ScalarFunctionFilterTransformer::class, $bound);
        static::assertSame(
            [['a' => 2], ['a' => 3]],
            $bound
                ->transform(array_to_rows([
                    ['a' => 1],
                    ['a' => 2],
                    ['a' => 3],
                ], $input), flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new ScalarFunctionFilterTransformer(ref('id')->greaterThan(lit(1)));
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }
}
