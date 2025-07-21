<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class PrependTest extends FlowTestCase
{
    public function test_prepend_empty_string_to_content() : void
    {
        $result = ref('str')->prepend('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_to_empty_string() : void
    {
        $result = ref('str')->prepend('hello')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_to_non_empty_string() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_prepend_with_null_prefix() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', null)
            )
        );

        self::assertEquals('world', $result);
    }

    public function test_prepend_with_null_value() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_prepend_with_scalar_function_parameter() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', 'Hello ')
            )
        );

        self::assertEquals('Hello world', $result);
    }
}
