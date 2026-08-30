<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class IsUtf8Test extends FlowTestCase
{
    public function test_is_utf8_returns_null(): void
    {
        static::assertNull(ref('str')->isUtf8()->eval(row(['str' => null]), flow_context()));
    }

    public function test_is_utf_8(): void
    {
        static::assertTrue(ref('str')->isUtf8()->eval(row(['str' => 'Lorem Ipsum']), flow_context()));

        static::assertFalse(ref('str')->isUtf8()->eval(row(['str' => "\xc3\x28"]), flow_context()));
    }
}
