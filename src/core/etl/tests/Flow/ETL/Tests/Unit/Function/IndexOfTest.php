<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of() : void
    {
        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa'))
            )
        );

        self::assertSame(
            0,
            ref('str')->indexOf('A', ignoreCase: true)->eval(
                row(str_entry('str', 'abbbbb'))
            )
        );

        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa'))
            )
        );

        self::assertNull(
            ref('str')->indexOf('x', offset: 2)->eval(
                row(str_entry('str', 'Abba'))
            )
        );
    }

    public function test_needle_null_index_of_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOf(ref('needle'))->eval(
                row(
                    str_entry('str', 'x'),
                    str_entry('needle', null)
                )
            )
        );
    }

    public function test_string_null_index_of_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOf('x')->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
