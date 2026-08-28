<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\regex_replace;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class RegexReplaceTest extends FlowTestCase
{
    public function test_a_null_limit_still_means_unlimited(): void
    {
        static::assertSame('x x x', regex_replace(lit('/a/'), lit('x'), ref('text'))->eval(
            row(str_entry('text', 'a a a')),
            flow_context(),
        ));
    }

    public function test_a_limit_caps_the_replacements(): void
    {
        static::assertSame('x a a', regex_replace(lit('/a/'), lit('x'), ref('text'), lit(1))->eval(
            row(str_entry('text', 'a a a')),
            flow_context(),
        ));
    }
}
