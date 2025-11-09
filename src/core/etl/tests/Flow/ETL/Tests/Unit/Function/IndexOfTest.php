<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of() : void
    {
        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa')),
                flow_context()
            )
        );

        self::assertSame(
            0,
            ref('str')->indexOf('A', ignoreCase: true)->eval(
                row(str_entry('str', 'abbbbb')),
                flow_context()
            )
        );

        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa')),
                flow_context()
            )
        );

        self::assertNull(
            ref('str')->indexOf('x', offset: 2)->eval(
                row(str_entry('str', 'Abba')),
                flow_context()
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
                ),
                flow_context()
            )
        );
    }

    public function test_string_null_index_of_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOf('x')->eval(
                row(
                    str_entry('str', null),
                ),
                flow_context()
            )
        );
    }
}
