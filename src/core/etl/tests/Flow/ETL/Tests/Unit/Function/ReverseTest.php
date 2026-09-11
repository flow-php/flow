<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ReverseTest extends FlowTestCase
{
    public function test_reverse_ascii_string(): void
    {
        static::assertSame('olleh', ref('str')->reverse()->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_reverse_empty_string(): void
    {
        static::assertSame('', ref('str')->reverse()->eval(row(['str' => '']), flow_context()));
    }

    public function test_reverse_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Reverse function requires non-null value');

        ref('str')->reverse()->eval(row(['str' => null]), flow_context());
    }
}
