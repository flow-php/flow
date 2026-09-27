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

final class EnsureEndTest extends FlowTestCase
{
    public function test_empty_string_with_suffix(): void
    {
        $result = ref('str')
            ->ensureEnd('_suffix')
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('_suffix', $result);
    }

    public function test_null_suffix(): void
    {
        $result = ref('str')
            ->ensureEnd(ref('suffix'))
            ->eval(
                array_to_row(
                    ['str' => 'hello', 'suffix' => null],
                    schema(str_schema('str'), str_schema('suffix', nullable: true)),
                ),
                flow_context(),
            );

        static::assertEquals('hello', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureEnd function requires non-null value');

        $result = ref('str')
            ->ensureEnd('_suffix')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_string_already_ends_with_suffix(): void
    {
        $result = ref('str')
            ->ensureEnd('.txt')
            ->eval(array_to_row(['str' => 'document.txt'], schema(str_schema('str'))), flow_context());

        static::assertEquals('document.txt', $result);
    }

    public function test_string_doesnt_end_with_suffix(): void
    {
        $result = ref('str')
            ->ensureEnd('.txt')
            ->eval(array_to_row(['str' => 'document'], schema(str_schema('str'))), flow_context());

        static::assertEquals('document.txt', $result);
    }

    public function test_string_with_empty_suffix(): void
    {
        $result = ref('str')
            ->ensureEnd('')
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->ensureEnd(ref('suffix'))
            ->eval(
                array_to_row(
                    ['str' => 'document', 'suffix' => '.pdf'],
                    schema(str_schema('str'), str_schema('suffix')),
                ),
                flow_context(),
            );

        static::assertEquals('document.pdf', $result);
    }
}
