<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty_empty_string() : void
    {
        self::assertTrue(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', '')),
                flow_context()
            )
        );
    }

    public function test_is_empty_non_empty_string() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'hello')),
                flow_context()
            )
        );
    }

    public function test_is_empty_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', null)),
                flow_context()
            )
        );
    }

    public function test_is_empty_single_character_string() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'a')),
                flow_context()
            )
        );
    }
}
