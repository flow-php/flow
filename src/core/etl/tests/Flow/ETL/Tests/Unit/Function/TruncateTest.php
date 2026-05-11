<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class TruncateTest extends FlowTestCase
{
    public function test_truncate_ellipsis_longer_than_limit(): void
    {
        static::assertSame('hello', ref('str')
            ->truncate(5, 'verylongellipsis')
            ->eval(row(str_entry('str', 'hello world')), flow_context()));
    }

    public function test_truncate_empty_string(): void
    {
        static::assertSame('', ref('str')->truncate(10)->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_truncate_exact_length(): void
    {
        static::assertSame('hello', ref('str')->truncate(5)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_truncate_returns_null_for_null_input(): void
    {
        static::assertNull(ref('str')->truncate(10)->eval(row(str_entry('str', null)), flow_context()));
    }

    public function test_truncate_string_longer_than_limit(): void
    {
        static::assertSame('he...', ref('str')
            ->truncate(5)
            ->eval(row(str_entry('str', 'hello world')), flow_context()));
    }

    public function test_truncate_string_shorter_than_limit(): void
    {
        static::assertSame('hello', ref('str')->truncate(10)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_truncate_with_custom_ellipsis(): void
    {
        static::assertSame('he***', ref('str')
            ->truncate(5, '***')
            ->eval(row(str_entry('str', 'hello world')), flow_context()));
    }

    public function test_truncate_with_length_one(): void
    {
        static::assertSame('h', ref('str')->truncate(1)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_truncate_with_length_zero(): void
    {
        static::assertSame('', ref('str')->truncate(0)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_truncate_with_negative_length(): void
    {
        static::assertSame('hell', ref('str')->truncate(-1)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_truncate_with_null_length(): void
    {
        static::assertSame('hello world', ref('str')
            ->truncate(ref('length'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('length', null)), flow_context()));
    }

    public function test_truncate_with_scalar_function_ellipsis(): void
    {
        static::assertSame('hel>>', ref('str')
            ->truncate(5, ref('ellipsis'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('ellipsis', '>>')), flow_context()));
    }

    public function test_truncate_with_scalar_function_length(): void
    {
        static::assertSame('he...', ref('str')
            ->truncate(ref('length'))
            ->eval(row(str_entry('str', 'hello world'), int_entry('length', 5)), flow_context()));
    }
}
