<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UntilTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class UntilTransformerTest extends FlowTestCase
{
    public function test_a_batch_that_holds_the_predicate_throughout_passes_through(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new UntilTransformer(ref('id')->lessThan(lit(3))))
                ->transform(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])), flow_context(config()))
                ->toArray(),
        );
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
        $thrown = null;

        try {
            (new UntilTransformer(ref('id')->lessThan(lit(3))))->transform(
                rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
                flow_context(config()),
            );
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame([['id' => 1], ['id' => 2]], $thrown->rows?->toArray());
    }

    public function test_it_stops_at_the_first_row_failing_the_predicate(): void
    {
        $thrown = null;

        try {
            (new UntilTransformer(ref('id')->lessThan(lit(3))))->transform(
                rows(
                    schema(int_schema('id')),
                    row(['id' => 1]),
                    row(['id' => 2]),
                    row(['id' => 5]),
                    row(['id' => 1]),
                    row(['id' => 2]),
                ),
                flow_context(config()),
            );
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame([['id' => 1], ['id' => 2]], $thrown->rows?->toArray());
    }

    public function test_a_reached_limit_stops_the_next_batch(): void
    {
        $transformer = new UntilTransformer(ref('id')->lessThan(lit(1)));

        try {
            $transformer->transform(rows(schema(int_schema('id')), row(['id' => 5])), flow_context(config()));
        } catch (LimitReachedException) {
        }

        $thrown = null;

        try {
            $transformer->transform(rows(schema(int_schema('id')), row(['id' => 0])), flow_context(config()));
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertEquals(rows(schema(int_schema('id'))), $thrown->rows);
    }

    public function test_a_non_boolean_predicate_is_rejected_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('until() requires a predicate returning boolean');

        (new UntilTransformer(ref('id')))->transform(
            rows(schema(int_schema('id')), row(['id' => 1])),
            flow_context(config()),
        );
    }

    public function test_an_empty_batch_is_bound_against_its_own_schema(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new UntilTransformer(ref('missing')->equals(lit(1))))->transform(rows(schema()), flow_context(config()));
    }

    public function test_a_reached_limit_stops_an_empty_next_batch(): void
    {
        $transformer = new UntilTransformer(ref('id')->lessThan(lit(1)));

        try {
            $transformer->transform(rows(schema(int_schema('id')), row(['id' => 5])), flow_context(config()));
        } catch (LimitReachedException) {
        }

        $thrown = null;

        try {
            $transformer->transform(rows(schema()), flow_context(config()));
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame([], $thrown->rows?->toArray());
    }
}
