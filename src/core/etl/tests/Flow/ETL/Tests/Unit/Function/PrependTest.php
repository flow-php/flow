<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class PrependTest extends FlowTestCase
{
    public function test_prepend_empty_string_to_content() : void
    {
        $result = ref('str')->prepend('')->eval(
            row(str_entry('str', 'hello')),
            flow_context()
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_to_empty_string() : void
    {
        $result = ref('str')->prepend('hello')->eval(
            row(str_entry('str', '')),
            flow_context()
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_to_non_empty_string() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', 'world')),
            flow_context()
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_prepend_with_null_prefix() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', null)
            ),
            flow_context()
        );

        self::assertEquals('world', $result);
    }

    public function test_prepend_with_null_value() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', null)),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_prepend_with_scalar_function_parameter() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', 'Hello ')
            ),
            flow_context()
        );

        self::assertEquals('Hello world', $result);
    }
}
