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

final class CodePointLengthTest extends FlowTestCase
{
    public function test_code_point_length_ascii_string(): void
    {
        static::assertSame(5, ref('str')
            ->codePointLength()
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()));
    }

    public function test_code_point_length_empty_string(): void
    {
        static::assertSame(0, ref('str')
            ->codePointLength()
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()));
    }

    public function test_code_point_length_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CodePointLength function requires non-null value');

        ref('str')
            ->codePointLength()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }
}
