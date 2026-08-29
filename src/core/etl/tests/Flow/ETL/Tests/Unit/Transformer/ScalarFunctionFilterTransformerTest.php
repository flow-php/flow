<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class ScalarFunctionFilterTransformerTest extends FlowTestCase
{
    public function test_equal(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->equals(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_equal_on_literal(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->equals(lit(2))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_greater_than(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThan(ref('a'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_greater_than_or_equal(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThanEqual(ref('a'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_less_than(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThan(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_less_than_equal(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThanEqual(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_not_equal(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notEquals(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_not_same(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notSame(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_same(): void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        static::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->same(ref('b'))))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_a_non_boolean_predicate_is_rejected_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('filter() requires a predicate returning boolean');

        (new ScalarFunctionFilterTransformer(ref('score')))->transform(
            rows(row(int_entry('score', 1)), row(int_entry('score', null))),
            flow_context(config()),
        );
    }

    public function test_a_null_propagating_predicate_over_a_nullable_column_is_accepted(): void
    {
        static::assertSame(
            [
                ['score' => 11],
            ],
            (new ScalarFunctionFilterTransformer(ref('score')->greaterThan(lit(10))))
                ->transform(
                    rows(row(int_entry('score', 11)), row(int_entry('score', 2)), row(int_entry('score', null))),
                    flow_context(config()),
                )
                ->toArray(),
        );
    }

    public function test_an_empty_batch_is_returned_without_binding(): void
    {
        $empty = rows();

        static::assertSame($empty, (new ScalarFunctionFilterTransformer(ref('missing')->equals(lit(1))))->transform(
            $empty,
            flow_context(config()),
        ));
    }
}
