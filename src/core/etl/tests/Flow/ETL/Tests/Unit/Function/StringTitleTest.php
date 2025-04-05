<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringTitleTest extends FlowTestCase
{
    public function test_string_title() : void
    {
        self::assertSame(
            'Foo ijssel',
            ref('str')->stringTitle()->eval(
                row(str_entry('str', 'foo ijssel'))
            )
        );
    }

    public function test_string_title_allwords() : void
    {
        self::assertSame(
            'Foo Ijssel',
            ref('str')->stringTitle(allWords: true)->eval(
                row(str_entry('str', 'foo ijssel'))
            )
        );
    }

    public function test_string_title_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringTitle()->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
