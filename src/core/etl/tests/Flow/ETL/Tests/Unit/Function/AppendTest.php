<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content(): void
    {
        $result = ref('str')->append('')->eval(row(str_entry('str', 'hello')), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_empty_string(): void
    {
        $result = ref('str')->append('hello')->eval(row(str_entry('str', '')), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string(): void
    {
        $result = ref('str')->append(' world')->eval(row(str_entry('str', 'hello')), flow_context());

        static::assertEquals('hello world', $result);
    }

    public function test_append_with_null_suffix(): void
    {
        $result = ref('str')
            ->append(ref('suffix'))
            ->eval(row(str_entry('str', 'hello'), str_entry('suffix', null)), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_with_null_value(): void
    {
        $result = ref('str')->append(' world')->eval(row(str_entry('str', null)), flow_context());

        static::assertNull($result);
    }

    public function test_append_with_null_value_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('str')->append(' world')->eval(row(str_entry('str', null)), $context);
    }

    public function test_append_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->append(ref('suffix'))
            ->eval(row(str_entry('str', 'hello'), str_entry('suffix', ' world')), flow_context());

        static::assertEquals('hello world', $result);
    }
}
