<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content() : void
    {
        $result = ref('str')->append('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_to_empty_string() : void
    {
        $result = ref('str')->append('hello')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello world', $result);
    }

    public function test_append_with_null_suffix() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_with_null_value() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_append_with_scalar_function_parameter() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', ' world')
            )
        );

        self::assertEquals('hello world', $result);
    }
}
