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

final class PrependTest extends FlowTestCase
{
    public function test_prepend_empty_string_to_content(): void
    {
        $result = ref('str')
            ->prepend('')
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_prepend_to_empty_string(): void
    {
        $result = ref('str')
            ->prepend('hello')
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_prepend_to_non_empty_string(): void
    {
        $result = ref('str')
            ->prepend('Hello ')
            ->eval(array_to_row(['str' => 'world'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world', $result);
    }

    public function test_prepend_with_null_prefix(): void
    {
        $result = ref('str')
            ->prepend(ref('prefix'))
            ->eval(
                array_to_row(
                    ['str' => 'world', 'prefix' => null],
                    schema(str_schema('str'), str_schema('prefix', nullable: true)),
                ),
                flow_context(),
            );

        static::assertEquals('world', $result);
    }

    public function test_prepend_with_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Prepend function requires non-null value');

        $result = ref('str')
            ->prepend('Hello ')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_prepend_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->prepend(ref('prefix'))
            ->eval(
                array_to_row(['str' => 'world', 'prefix' => 'Hello '], schema(str_schema('str'), str_schema('prefix'))),
                flow_context(),
            );

        static::assertEquals('Hello world', $result);
    }
}
