<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class TruncateTest extends FlowTestCase
{
    public function test_truncate_ellipsis_longer_than_limit(): void
    {
        static::assertSame('hello', ref('str')
            ->truncate(5, 'verylongellipsis')
            ->eval(row(['str' => 'hello world']), flow_context()));
    }

    public function test_truncate_empty_string(): void
    {
        static::assertSame('', ref('str')->truncate(10)->eval(row(['str' => '']), flow_context()));
    }

    public function test_truncate_exact_length(): void
    {
        static::assertSame('hello', ref('str')->truncate(5)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_truncate_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Truncate function requires non-null value');

        ref('str')->truncate(10)->eval(row(['str' => null]), flow_context());
    }

    public function test_truncate_string_longer_than_limit(): void
    {
        static::assertSame('he...', ref('str')->truncate(5)->eval(row(['str' => 'hello world']), flow_context()));
    }

    public function test_truncate_string_shorter_than_limit(): void
    {
        static::assertSame('hello', ref('str')->truncate(10)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_truncate_with_custom_ellipsis(): void
    {
        static::assertSame('he***', ref('str')
            ->truncate(5, '***')
            ->eval(row(['str' => 'hello world']), flow_context()));
    }

    public function test_truncate_with_length_one(): void
    {
        static::assertSame('h', ref('str')->truncate(1)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_truncate_with_length_zero(): void
    {
        static::assertSame('', ref('str')->truncate(0)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_truncate_with_negative_length(): void
    {
        static::assertSame('hell', ref('str')->truncate(-1)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_truncate_with_null_length(): void
    {
        static::assertSame('hello world', ref('str')
            ->truncate(ref('length'))
            ->eval(row(['str' => 'hello world', 'length' => null]), flow_context()));
    }

    public function test_truncate_with_scalar_function_ellipsis(): void
    {
        static::assertSame('hel>>', ref('str')
            ->truncate(5, ref('ellipsis'))
            ->eval(row(['str' => 'hello world', 'ellipsis' => '>>']), flow_context()));
    }

    public function test_truncate_with_scalar_function_length(): void
    {
        static::assertSame('he...', ref('str')
            ->truncate(ref('length'))
            ->eval(row(['str' => 'hello world', 'length' => 5]), flow_context()));
    }
}
