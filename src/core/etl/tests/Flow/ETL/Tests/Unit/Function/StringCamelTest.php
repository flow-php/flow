<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class StringCamelTest extends FlowTestCase
{
    public function test_string_folded() : void
    {
        self::assertSame(
            'fooBarBaz',
            ref('str')->stringCamel()->eval(
                row(str_entry('str', 'Foo: Bar-baz.'))
            )
        );
    }
}
