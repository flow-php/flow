<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\Transformer\UntilTransformer;
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

final class UntilTransformerTest extends FlowTestCase
{
    public function test_a_batch_that_holds_the_predicate_throughout_passes_through(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(3)));

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            $until
                ->transform(array_to_rows([
                    ['id' => 1],
                    ['id' => 2],
                ], schema(int_schema('id'))), flow_context(config())->backend())
                ->toArray(),
        );
        static::assertFalse($until->reached());
    }

    public function test_bind_refuses_a_non_boolean_predicate(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new UntilTransformer(ref('id')))->bind(schema(int_schema('id')));
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals($input, (new UntilTransformer(ref('id')->lessThan(lit(3))))->bind($input)->output);
    }

    public function test_passes_rows_until_the_predicate_stops_holding(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(3)));

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            $until
                ->transform(array_to_rows([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                ], schema(int_schema('id'))), flow_context(config())->backend())
                ->toArray(),
        );
        static::assertTrue($until->reached());
    }

    public function test_it_stops_at_the_first_row_failing_the_predicate(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(3)));

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            $until
                ->transform(array_to_rows([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 5],
                    ['id' => 1],
                    ['id' => 2],
                ], schema(int_schema('id'))), flow_context(config())->backend())
                ->toArray(),
        );
        static::assertTrue($until->reached());
    }

    public function test_a_first_row_failing_the_predicate_empties_the_batch(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(1)));

        static::assertSame(
            [],
            $until
                ->transform(array_to_rows([
                    ['id' => 5],
                    ['id' => 0],
                ], schema(int_schema('id'))), flow_context(config())->backend())
                ->toArray(),
        );
        static::assertTrue($until->reached());
    }

    public function test_a_reached_limit_stops_the_next_batch(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(1)));
        $until->transform(array_to_rows([[
            'id' => 5,
        ]], schema(int_schema('id'))), flow_context(config())->backend());

        static::assertEquals(
            rows(schema(int_schema('id'))),
            $until->transform(array_to_rows([[
                'id' => 0,
            ]], schema(int_schema('id'))), flow_context(config())->backend()),
        );
        static::assertTrue($until->reached());
    }

    public function test_a_non_boolean_predicate_is_rejected_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('until() requires a predicate returning boolean');

        (new UntilTransformer(ref('id')))->transform(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), flow_context(config())->backend());
    }

    public function test_an_empty_batch_is_bound_against_its_own_schema(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new UntilTransformer(ref('missing')->equals(lit(1))))->transform(
            rows(schema()),
            flow_context(config())->backend(),
        );
    }

    public function test_a_reached_limit_stops_an_empty_next_batch(): void
    {
        $until = new UntilTransformer(ref('id')->lessThan(lit(1)));
        $until->transform(array_to_rows([[
            'id' => 5,
        ]], schema(int_schema('id'))), flow_context(config())->backend());

        static::assertSame([], $until->transform(rows(schema()), flow_context(config())->backend())->toArray());
        static::assertTrue($until->reached());
    }

    public function test_bind_refuses_an_expand_in_the_predicate(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'until() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        (new UntilTransformer(ref('tags')->expand()->equals(lit('x'))))->bind(ListColumnsMother::schema());
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new UntilTransformer(ref('id')->lessThan(lit(10)));
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
