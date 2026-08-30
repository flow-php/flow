<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content(): void
    {
        $result = ref('str')->append('')->eval(row(['str' => 'hello']), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_empty_string(): void
    {
        $result = ref('str')->append('hello')->eval(row(['str' => '']), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string(): void
    {
        $result = ref('str')->append(' world')->eval(row(['str' => 'hello']), flow_context());

        static::assertEquals('hello world', $result);
    }

    public function test_append_with_null_suffix(): void
    {
        $result = ref('str')->append(ref('suffix'))->eval(row(['str' => 'hello', 'suffix' => null]), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_with_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $result = ref('str')->append(' world')->eval(row(['str' => null]), flow_context());

        static::assertNull($result);
    }

    public function test_append_with_null_value_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $context = flow_context(config());
        ref('str')->append(' world')->eval(row(['str' => null]), $context);
    }

    public function test_append_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->append(ref('suffix'))
            ->eval(row(['str' => 'hello', 'suffix' => ' world']), flow_context());

        static::assertEquals('hello world', $result);
    }
}
