<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringAfterTest extends FlowTestCase
{
    public function test_string_after() : void
    {
        self::assertSame(
            ' world',
            ref('str')->stringAfter(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'hello')
                )
            )
        );

        self::assertSame(
            ' world',
            ref('str')->stringAfter(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_including_needle() : void
    {
        self::assertSame(
            'o world',
            ref('str')->stringAfter(ref('needle'), includeNeedle: true)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringAfter('x')->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
