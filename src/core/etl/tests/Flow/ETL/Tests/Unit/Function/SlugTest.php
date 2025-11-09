<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, row, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class SlugTest extends FlowTestCase
{
    public function test_ascii_on_null() : void
    {
        self::assertNull(
            ref('str')->slug()->eval(row(str_entry('str', null)), flow_context())
        );
    }

    public function test_slug() : void
    {
        self::assertSame(
            'azcz',
            ref('str')->slug()->eval(row(str_entry('str', 'ąźćż')), flow_context())
        );
    }

    public function test_slug_separator() : void
    {
        self::assertSame(
            'Some_Text',
            ref('str')->slug('_')->eval(row(str_entry('str', 'Some Text')), flow_context())
        );
    }
}
