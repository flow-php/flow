<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content(): void
    {
        $result = ref('str')
            ->append('')
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_empty_string(): void
    {
        $result = ref('str')
            ->append('hello')
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string(): void
    {
        $result = ref('str')
            ->append(' world')
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello world', $result);
    }

    public function test_append_with_null_suffix(): void
    {
        $result = ref('str')
            ->append(ref('suffix'))
            ->eval(
                array_to_row(
                    ['str' => 'hello', 'suffix' => null],
                    schema(str_schema('str'), str_schema('suffix', nullable: true)),
                ),
                flow_context(),
            );

        static::assertEquals('hello', $result);
    }

    public function test_append_with_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $result = ref('str')
            ->append(' world')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_append_with_null_value_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $context = flow_context(config());
        ref('str')
            ->append(' world')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), $context);
    }

    public function test_append_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->append(ref('suffix'))
            ->eval(
                array_to_row(['str' => 'hello', 'suffix' => ' world'], schema(str_schema('str'), str_schema('suffix'))),
                flow_context(),
            );

        static::assertEquals('hello world', $result);
    }
}
