<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty_empty_string(): void
    {
        static::assertTrue(
            ref('str')->isEmpty()->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_is_empty_non_empty_string(): void
    {
        static::assertFalse(
            ref('str')->isEmpty()->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_is_empty_returns_null_for_null_input(): void
    {
        static::assertNull(
            ref('str')
                ->isEmpty()
                ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context()),
        );
    }

    public function test_is_empty_single_character_string(): void
    {
        static::assertFalse(
            ref('str')->isEmpty()->eval(array_to_row(['str' => 'a'], schema(str_schema('str'))), flow_context()),
        );
    }
}
