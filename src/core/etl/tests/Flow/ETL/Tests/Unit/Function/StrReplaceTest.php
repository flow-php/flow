<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class StrReplaceTest extends FlowTestCase
{
    public function test_str_replace_on_valid_string() : void
    {
        self::assertSame(
            '1',
            ref('value')->strReplace('test', '1')->eval(row(str_entry('value', 'test'))),
        );
    }

    public function test_str_replace_on_valid_string_with_array_of_replacements() : void
    {
        self::assertSame(
            'test was successful',
            ref('value')->strReplace(['is', 'broken'], ['was', 'successful'])->eval(row(str_entry('value', 'test is broken'))),
        );
    }
}
