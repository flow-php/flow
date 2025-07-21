<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{bool_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast('l')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_not_found() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('x')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_null_needle_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOfLast(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('needle', null)
                )
            )
        );
    }

    public function test_index_of_last_null_string_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOfLast('l')->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_index_of_last_with_scalar_function_parameters() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast(ref('needle'), ref('ignore_case'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'L'),
                    bool_entry('ignore_case', true)
                )
            )
        );
    }
}
