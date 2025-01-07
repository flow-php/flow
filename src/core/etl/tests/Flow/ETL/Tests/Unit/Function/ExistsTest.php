<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, ref, str_entry};
use Flow\ETL\Function\Exists;
use Flow\ETL\Tests\FlowTestCase;

final class ExistsTest extends FlowTestCase
{
    public function test_if_reference_exists() : void
    {
        self::assertTrue(
            ref('value')->exists()->eval(row(str_entry('value', 'test')))
        );
    }

    public function test_that_lit_function_exists() : void
    {
        self::assertTrue(
            (new Exists(lit('val')))->eval(row())
        );
    }

    public function test_that_null_reference_to_null_entry_exists() : void
    {
        self::assertTrue(
            ref('value')->exists()->eval(row(str_entry('value', null)))
        );
    }

    public function test_that_reference_does_not_exists() : void
    {
        self::assertFalse(
            ref('value')->exists()->eval(row())
        );
    }
}
