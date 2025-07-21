<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{json_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringContainsAnyTest extends FlowTestCase
{
    public function test_contains_any_empty_needles_array() : void
    {
        self::assertFalse(
            ref('str')->stringContainsAny([])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_multiple_needles_one_found() : void
    {
        self::assertTrue(
            ref('str')->stringContainsAny(['foo', 'world', 'bar'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_no_needles_found() : void
    {
        self::assertFalse(
            ref('str')->stringContainsAny(['foo', 'bar', 'baz'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_null_needles() : void
    {
        self::assertFalse(
            ref('str')->stringContainsAny(ref('needles'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    json_entry('needles', null)
                )
            )
        );
    }

    public function test_contains_any_null_string() : void
    {
        self::assertFalse(
            ref('str')->stringContainsAny(['hello', 'world'])->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_contains_any_with_scalar_function_parameter() : void
    {
        self::assertTrue(
            ref('str')->stringContainsAny(ref('needles'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    json_entry('needles', ['world', 'foo'])
                )
            )
        );
    }
}
