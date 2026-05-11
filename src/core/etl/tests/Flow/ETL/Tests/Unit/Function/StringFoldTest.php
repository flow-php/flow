<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringFoldTest extends FlowTestCase
{
    public function test_string_folded(): void
    {
        static::assertSame("die o'brian strasse", ref('str')
            ->stringFold()
            ->eval(row(str_entry('str', "Die O'Brian Straße")), flow_context()));
    }

    public function test_string_folded_returns_null(): void
    {
        static::assertNull(ref('str')->stringFold()->eval(row(str_entry('str', null)), flow_context()));
    }
}
