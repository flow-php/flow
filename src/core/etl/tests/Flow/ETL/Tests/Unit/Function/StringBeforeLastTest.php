<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringBeforeLastTest extends FlowTestCase
{
    public function test_string_before_last() : void
    {
        self::assertSame(
            'hello w',
            ref('str')->stringBeforeLast(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_before_last_including_needle() : void
    {
        self::assertSame(
            'hello wo',
            ref('str')->stringBeforeLast(ref('needle'), includeNeedle: true)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_before_last_returns_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->stringBeforeLast(ref('needle'))->eval(
                row(
                    str_entry('str', ''),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_before_last_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringBeforeLast(ref('needle'))->eval(
                row(
                    str_entry('str', null),
                    str_entry('needle', 'o')
                )
            )
        );
    }
}
