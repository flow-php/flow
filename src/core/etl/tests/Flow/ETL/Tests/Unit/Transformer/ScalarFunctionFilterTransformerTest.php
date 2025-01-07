<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, flow_context, row, rows};
use function Flow\ETL\DSL\{int_entry, lit, ref};
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class ScalarFunctionFilterTransformerTest extends FlowTestCase
{
    public function test_equal() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(
                ref('a')->equals(ref('b'))
            ))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_equal_on_literal() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->equals(lit(2))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_greater_than() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThan(ref('a'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_greater_than_or_equal() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('b')->greaterThanEqual(ref('a'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_less_than() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThan(ref('b'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_less_than_equal() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->lessThanEqual(ref('b'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_not_equal() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notEquals(ref('b'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_not_same() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->notSame(ref('b'))))->transform($rows, flow_context(config()))->toArray()
        );
    }

    public function test_same() : void
    {
        $rows = rows(row(int_entry('a', 1), int_entry('b', 1)), row(int_entry('a', 1), int_entry('b', 2)));

        self::assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new ScalarFunctionFilterTransformer(ref('a')->same(ref('b'))))->transform($rows, flow_context(config()))->toArray()
        );
    }
}
