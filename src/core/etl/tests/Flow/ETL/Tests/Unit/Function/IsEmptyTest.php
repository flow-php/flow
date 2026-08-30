<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty_empty_string(): void
    {
        static::assertTrue(ref('str')->isEmpty()->eval(row(['str' => '']), flow_context()));
    }

    public function test_is_empty_non_empty_string(): void
    {
        static::assertFalse(ref('str')->isEmpty()->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_is_empty_returns_null_for_null_input(): void
    {
        static::assertNull(ref('str')->isEmpty()->eval(row(['str' => null]), flow_context()));
    }

    public function test_is_empty_single_character_string(): void
    {
        static::assertFalse(ref('str')->isEmpty()->eval(row(['str' => 'a']), flow_context()));
    }
}
