<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Function\Exists;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ExistsTest extends TestCase
{
    public function test_if_reference_exists() : void
    {
        $this->assertTrue(
            ref('value')->exists()->eval(Row::create(str_entry('value', 'test')))
        );
    }

    public function test_that_lit_function_exists() : void
    {
        $this->assertTrue(
            (new Exists(lit('val')))->eval(Row::create())
        );
    }

    public function test_that_null_reference_to_null_entry_exists() : void
    {
        $this->assertTrue(
            ref('value')->exists()->eval(Row::create(null_entry('value')))
        );
    }

    public function test_that_reference_does_not_exists() : void
    {
        $this->assertFalse(
            ref('value')->exists()->eval(Row::create())
        );
    }
}
