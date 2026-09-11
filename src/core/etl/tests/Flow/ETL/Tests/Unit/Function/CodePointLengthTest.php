<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class CodePointLengthTest extends FlowTestCase
{
    public function test_code_point_length_ascii_string(): void
    {
        static::assertSame(5, ref('str')->codePointLength()->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_code_point_length_empty_string(): void
    {
        static::assertSame(0, ref('str')->codePointLength()->eval(row(['str' => '']), flow_context()));
    }

    public function test_code_point_length_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CodePointLength function requires non-null value');

        ref('str')->codePointLength()->eval(row(['str' => null]), flow_context());
    }
}
