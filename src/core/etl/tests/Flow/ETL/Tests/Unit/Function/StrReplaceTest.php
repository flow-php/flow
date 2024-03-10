<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class StrReplaceTest extends TestCase
{
    public function test_str_replace_on_non_string_value() : void
    {
        self::assertNull(
            ref('value')->strReplace('test', '1')->eval(Row::create(int_entry('value', 1000))),
        );
    }

    public function test_str_replace_on_valid_string() : void
    {
        self::assertSame(
            '1',
            ref('value')->strReplace('test', '1')->eval(Row::create(str_entry('value', 'test'))),
        );
    }

    public function test_str_replace_on_valid_string_with_array_of_replacements() : void
    {
        self::assertSame(
            'test was successful',
            ref('value')->strReplace(['is', 'broken'], ['was', 'successful'])->eval(Row::create(str_entry('value', 'test is broken'))),
        );
    }
}
