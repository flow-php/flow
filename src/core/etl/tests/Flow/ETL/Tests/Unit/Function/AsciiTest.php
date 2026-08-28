<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AsciiTest extends FlowTestCase
{
    public function test_ascii(): void
    {
        static::assertSame('azcz', ref('str')->ascii()->eval(row(str_entry('str', 'ąźćż')), flow_context()));
    }

    public function test_ascii_on_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ascii function requires non-null value');

        ref('str')->ascii()->eval(row(str_entry('str', null)), flow_context());
    }
}
