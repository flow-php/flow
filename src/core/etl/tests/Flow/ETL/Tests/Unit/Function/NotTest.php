<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class NotTest extends TestCase
{
    public function test_not_expression_on_array_true_value() : void
    {
        $this->assertFalse(
            not(lit([1, 2, 3]))->eval(Row::create())
        );
    }

    public function test_not_expression_on_boolean_true_value() : void
    {
        $this->assertFalse(
            not(lit(true))->eval(Row::create())
        );
    }

    public function test_not_expression_on_is_in_expression() : void
    {
        $this->assertTrue(
            not(ref('value')->isIn(ref('array')))->eval(Row::create(array_entry('array', [1, 2, 3]), int_entry('value', 10)))
        );
    }
}
