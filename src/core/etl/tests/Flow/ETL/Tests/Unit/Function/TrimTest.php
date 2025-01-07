<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry};
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Tests\FlowTestCase;

final class TrimTest extends FlowTestCase
{
    public function test_trim_both_valid_string() : void
    {
        self::assertSame(
            'value',
            ref('string')->trim()->eval(row(str_entry('string', '   value')))
        );
    }

    public function test_trim_left_valid_string() : void
    {
        self::assertSame(
            'value   ',
            ref('string')->trim(Type::LEFT)->eval(row(str_entry('string', '   value   ')))
        );
    }

    public function test_trim_right_valid_string() : void
    {
        self::assertSame(
            '   value',
            ref('string')->trim(Type::RIGHT)->eval(row(str_entry('string', '   value   ')))
        );
    }
}
