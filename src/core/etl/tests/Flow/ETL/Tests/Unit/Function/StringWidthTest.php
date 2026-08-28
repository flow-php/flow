<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringWidthTest extends FlowTestCase
{
    public function test_width_ascii_string(): void
    {
        static::assertSame(5, ref('str')->stringWidth()->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_width_empty_string(): void
    {
        static::assertSame(0, ref('str')->stringWidth()->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_width_returns_null_for_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringWidth function requires non-null value');

        ref('str')->stringWidth()->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_width_single_character(): void
    {
        static::assertSame(1, ref('str')->stringWidth()->eval(row(str_entry('str', 'a')), flow_context()));
    }
}
