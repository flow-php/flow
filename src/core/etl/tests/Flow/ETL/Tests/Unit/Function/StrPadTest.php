<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class StrPadTest extends TestCase
{
    public function test_str_pad_on_non_string_value() : void
    {
        $this->assertNull(
            ref('value')->strPad(5, '-', \STR_PAD_LEFT)->eval(Row::create(Entry::int('value', 1000))),
        );
    }

    public function test_str_pad_on_valid_string() : void
    {
        $this->assertSame(
            '----N',
            ref('value')->strPad(5, '-', \STR_PAD_LEFT)->eval(Row::create(Entry::str('value', 'N'))),
        );
    }
}
