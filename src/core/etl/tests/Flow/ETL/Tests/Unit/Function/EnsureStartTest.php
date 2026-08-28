<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class EnsureStartTest extends FlowTestCase
{
    public function test_empty_string_with_prefix(): void
    {
        $result = ref('str')->ensureStart('prefix_')->eval(row(str_entry('str', '')), flow_context());

        static::assertEquals('prefix_', $result);
    }

    public function test_null_prefix(): void
    {
        $result = ref('str')
            ->ensureStart(ref('prefix'))
            ->eval(row(str_entry('str', 'hello'), str_entry('prefix', null)), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureStart function requires non-null value');

        $result = ref('str')->ensureStart('prefix_')->eval(row(str_entry('str', null)), flow_context());

        static::assertNull($result);
    }

    public function test_string_already_starts_with_prefix(): void
    {
        $result = ref('str')
            ->ensureStart('https://')
            ->eval(row(str_entry('str', 'https://example.com')), flow_context());

        static::assertEquals('https://example.com', $result);
    }

    public function test_string_doesnt_start_with_prefix(): void
    {
        $result = ref('str')->ensureStart('https://')->eval(row(str_entry('str', 'example.com')), flow_context());

        static::assertEquals('https://example.com', $result);
    }

    public function test_string_with_empty_prefix(): void
    {
        $result = ref('str')->ensureStart('')->eval(row(str_entry('str', 'hello')), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->ensureStart(ref('prefix'))
            ->eval(row(str_entry('str', 'example.com'), str_entry('prefix', 'https://')), flow_context());

        static::assertEquals('https://example.com', $result);
    }
}
