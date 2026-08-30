<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;

final class IsInTest extends FlowTestCase
{
    public function test_a_match_beats_a_null_element(): void
    {
        static::assertTrue(lit(5)->isIn(lit([1, null, 5]))->eval(row([]), flow_context()));
    }

    public function test_no_match_with_a_null_element_is_null(): void
    {
        static::assertNull(lit(7)->isIn(lit([1, null, 5]))->eval(row([]), flow_context()));
    }

    public function test_no_match_without_a_null_element_is_false(): void
    {
        static::assertFalse(lit(7)->isIn(lit([1, 3, 5]))->eval(row([]), flow_context()));
    }

    public function test_a_null_needle_is_null(): void
    {
        static::assertNull(lit(null)->isIn(lit([1, 3, 5]))->eval(row([]), flow_context()));
    }

    public function test_a_null_haystack_is_null(): void
    {
        static::assertNull(lit(5)->isIn(lit(null))->eval(row(['x' => []]), flow_context()));
    }
}
