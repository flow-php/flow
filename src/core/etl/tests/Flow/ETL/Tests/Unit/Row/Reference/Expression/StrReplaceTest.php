<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class StrReplaceTest extends TestCase
{
    public function test_str_replace_on_non_string_value() : void
    {
        $this->assertNull(
            ref('value')->strReplace('test', '1')->eval(Row::create(Entry::int('value', 1000))),
        );
    }

    public function test_str_replace_on_valid_string() : void
    {
        $this->assertSame(
            '1',
            ref('value')->strReplace('test', '1')->eval(Row::create(Entry::str('value', 'test'))),
        );
    }

    public function test_str_replace_on_valid_string_with_array_of_replacements() : void
    {
        $this->assertSame(
            'test was successful',
            ref('value')->strReplace(['is', 'broken'], ['was', 'successful'])->eval(Row::create(Entry::str('value', 'test is broken'))),
        );
    }
}
