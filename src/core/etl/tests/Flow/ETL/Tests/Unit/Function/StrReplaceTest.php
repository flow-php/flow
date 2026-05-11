<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StrReplaceTest extends FlowTestCase
{
    public function test_str_replace_on_valid_string(): void
    {
        static::assertSame('1', ref('value')
            ->strReplace('test', '1')
            ->eval(row(str_entry('value', 'test')), flow_context()));
    }

    public function test_str_replace_on_valid_string_with_array_of_replacements(): void
    {
        static::assertSame('test was successful', ref('value')
            ->strReplace(['is', 'broken'], ['was', 'successful'])
            ->eval(row(str_entry('value', 'test is broken')), flow_context()));
    }
}
