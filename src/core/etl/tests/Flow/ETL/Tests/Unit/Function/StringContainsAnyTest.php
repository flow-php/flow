<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringContainsAnyTest extends FlowTestCase
{
    public function test_contains_any_empty_needles_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires a non-empty needles array');

        ref('str')->stringContainsAny([])->eval(row(['str' => 'hello world']), flow_context());
    }

    public function test_contains_any_multiple_needles_one_found(): void
    {
        static::assertTrue(
            ref('str')->stringContainsAny(['foo', 'world', 'bar'])->eval(row(['str' => 'hello world']), flow_context()),
        );
    }

    public function test_contains_any_no_needles_found(): void
    {
        static::assertFalse(
            ref('str')->stringContainsAny(['foo', 'bar', 'baz'])->eval(row(['str' => 'hello world']), flow_context()),
        );
    }

    public function test_contains_any_null_needles(): void
    {
        static::assertNull(
            ref('str')
                ->stringContainsAny(ref('needles'))
                ->eval(row(['str' => 'hello world', 'needles' => null]), flow_context()),
        );
    }

    public function test_contains_any_null_string(): void
    {
        static::assertNull(
            ref('str')->stringContainsAny(['hello', 'world'])->eval(row(['str' => null]), flow_context()),
        );
    }

    public function test_contains_any_null_string_in_strict_mode(): void
    {
        $context = flow_context(config());
        static::assertNull(ref('str')->stringContainsAny(['hello', 'world'])->eval(row(['str' => null]), $context));
    }

    public function test_contains_any_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringContainsAny(ref('needles'))
                ->eval(row(['str' => 'hello world', 'needles' => ['world', 'foo']]), flow_context()),
        );
    }
}
