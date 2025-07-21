<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class EnsureStartTest extends FlowTestCase
{
    public function test_empty_string_with_prefix() : void
    {
        $result = ref('str')->ensureStart('prefix_')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('prefix_', $result);
    }

    public function test_null_prefix() : void
    {
        $result = ref('str')->ensureStart(ref('prefix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('prefix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->ensureStart('prefix_')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_string_already_starts_with_prefix() : void
    {
        $result = ref('str')->ensureStart('https://')->eval(
            row(str_entry('str', 'https://example.com'))
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_string_doesnt_start_with_prefix() : void
    {
        $result = ref('str')->ensureStart('https://')->eval(
            row(str_entry('str', 'example.com'))
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_string_with_empty_prefix() : void
    {
        $result = ref('str')->ensureStart('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->ensureStart(ref('prefix'))->eval(
            row(
                str_entry('str', 'example.com'),
                str_entry('prefix', 'https://')
            )
        );

        self::assertEquals('https://example.com', $result);
    }
}
