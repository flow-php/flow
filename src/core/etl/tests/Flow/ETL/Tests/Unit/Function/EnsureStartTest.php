<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class EnsureStartTest extends FlowTestCase
{
    public function test_empty_string_with_prefix(): void
    {
        $result = ref('str')
            ->ensureStart('prefix_')
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('prefix_', $result);
    }

    public function test_null_prefix(): void
    {
        $result = ref('str')
            ->ensureStart(ref('prefix'))
            ->eval(
                array_to_row(
                    ['str' => 'hello', 'prefix' => null],
                    schema(str_schema('str'), str_schema('prefix', nullable: true)),
                ),
                flow_context(),
            );

        static::assertEquals('hello', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureStart function requires non-null value');

        $result = ref('str')
            ->ensureStart('prefix_')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_string_already_starts_with_prefix(): void
    {
        $result = ref('str')
            ->ensureStart('https://')
            ->eval(array_to_row(['str' => 'https://example.com'], schema(str_schema('str'))), flow_context());

        static::assertEquals('https://example.com', $result);
    }

    public function test_string_doesnt_start_with_prefix(): void
    {
        $result = ref('str')
            ->ensureStart('https://')
            ->eval(array_to_row(['str' => 'example.com'], schema(str_schema('str'))), flow_context());

        static::assertEquals('https://example.com', $result);
    }

    public function test_string_with_empty_prefix(): void
    {
        $result = ref('str')
            ->ensureStart('')
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->ensureStart(ref('prefix'))
            ->eval(
                array_to_row(
                    ['str' => 'example.com', 'prefix' => 'https://'],
                    schema(str_schema('str'), str_schema('prefix')),
                ),
                flow_context(),
            );

        static::assertEquals('https://example.com', $result);
    }
}
