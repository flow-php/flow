<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringContainsAnyTest extends FlowTestCase
{
    public function test_contains_any_empty_needles_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires non-null, non-empty needles array');

        ref('str')->stringContainsAny([])->eval(row(str_entry('str', 'hello world')), flow_context());
    }

    public function test_contains_any_multiple_needles_one_found(): void
    {
        static::assertTrue(
            ref('str')
                ->stringContainsAny(['foo', 'world', 'bar'])
                ->eval(row(str_entry('str', 'hello world')), flow_context()),
        );
    }

    public function test_contains_any_no_needles_found(): void
    {
        static::assertFalse(
            ref('str')
                ->stringContainsAny(['foo', 'bar', 'baz'])
                ->eval(row(str_entry('str', 'hello world')), flow_context()),
        );
    }

    public function test_contains_any_null_needles(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires non-null, non-empty needles array');

        ref('str')
            ->stringContainsAny(ref('needles'))
            ->eval(row(str_entry('str', 'hello world'), json_entry('needles', null)), flow_context());
    }

    public function test_contains_any_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires non-null string');

        ref('str')->stringContainsAny(['hello', 'world'])->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_contains_any_null_string_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires non-null string');

        $context = flow_context(config());
        ref('str')->stringContainsAny(['hello', 'world'])->eval(row(str_entry('str', null)), $context);
    }

    public function test_contains_any_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringContainsAny(ref('needles'))
                ->eval(row(str_entry('str', 'hello world'), json_entry('needles', ['world', 'foo'])), flow_context()),
        );
    }
}
