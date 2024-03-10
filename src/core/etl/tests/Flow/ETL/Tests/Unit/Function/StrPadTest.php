<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class StrPadTest extends TestCase
{
    public function test_str_pad_on_non_string_value() : void
    {
        self::assertNull(
            ref('value')->strPad(5, '-', \STR_PAD_LEFT)->eval(Row::create(int_entry('value', 1000))),
        );
    }

    public function test_str_pad_on_valid_string() : void
    {
        self::assertSame(
            '----N',
            ref('value')->strPad(5, '-', \STR_PAD_LEFT)->eval(Row::create(str_entry('value', 'N'))),
        );
    }
}
