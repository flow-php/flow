<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IsUtf8Test extends FlowTestCase
{
    public function test_is_utf8_returns_null(): void
    {
        static::assertNull(
            ref('str')
                ->isUtf8()
                ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context()),
        );
    }

    public function test_is_utf_8(): void
    {
        static::assertTrue(
            ref('str')
                ->isUtf8()
                ->eval(array_to_row(['str' => 'Lorem Ipsum'], schema(str_schema('str'))), flow_context()),
        );

        static::assertFalse(
            ref('str')->isUtf8()->eval(array_to_row(['str' => "\xc3\x28"], schema(str_schema('str'))), flow_context()),
        );
    }
}
