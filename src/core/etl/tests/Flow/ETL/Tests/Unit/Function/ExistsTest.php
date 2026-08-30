<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Exists;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ExistsTest extends FlowTestCase
{
    public function test_a_throwing_operand_means_the_reference_does_not_exist(): void
    {
        static::assertFalse((new Exists(ref('value')->upper()))->eval(row(['value' => 1]), flow_context()));
    }

    public function test_if_reference_exists(): void
    {
        static::assertTrue(ref('value')->exists()->eval(row(['value' => 'test']), flow_context()));
    }

    public function test_that_lit_function_exists(): void
    {
        static::assertTrue((new Exists(lit('val')))->eval(row([]), flow_context()));
    }

    public function test_that_null_reference_to_null_entry_exists(): void
    {
        static::assertTrue(ref('value')->exists()->eval(row(['value' => null]), flow_context()));
    }

    public function test_that_reference_does_not_exists(): void
    {
        static::assertFalse(ref('value')->exists()->eval(row([]), flow_context()));
    }
}
