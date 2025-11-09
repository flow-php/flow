<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content() : void
    {
        $result = ref('str')->append('')->eval(
            row(str_entry('str', 'hello')),
            flow_context()
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_to_empty_string() : void
    {
        $result = ref('str')->append('hello')->eval(
            row(str_entry('str', '')),
            flow_context()
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', 'hello')),
            flow_context()
        );

        self::assertEquals('hello world', $result);
    }

    public function test_append_with_null_suffix() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', null)
            ),
            flow_context()
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_with_null_value() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', null)),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_append_with_null_value_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('str')->append(' world')->eval(
            row(str_entry('str', null)),
            $context
        );
    }

    public function test_append_with_scalar_function_parameter() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', ' world')
            ),
            flow_context()
        );

        self::assertEquals('hello world', $result);
    }
}
