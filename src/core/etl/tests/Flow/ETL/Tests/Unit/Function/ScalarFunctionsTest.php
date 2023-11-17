<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Function\Cast;
use Flow\ETL\Function\Equals;
use Flow\ETL\Function\ScalarFunctions;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ScalarFunctionsTest extends TestCase
{
    public function test_evaluating_multiple_entry_references() : void
    {
        $this->assertTrue(
            (new ScalarFunctions(
                ref('entry'),
                new Cast(ref('entry'), 'string'),
                new Equals(ref('entry'), lit(1))
            ))->eval(Row::create(
                Entry::int('entry', 1),
            ))
        );
    }

    public function test_evaluating_multiple_literal_functions() : void
    {
        $this->assertSame(
            'value3',
            (new ScalarFunctions(
                lit('value1'),
                lit('value2'),
                lit('value3'),
            ))->eval(Row::create())
        );
    }

    public function test_evaluation_cast_expression() : void
    {
        $this->assertSame(
            1,
            (new ScalarFunctions(new Cast(ref('entry'), 'int')))->eval(Row::create(Entry::string('entry', '1')))
        );
    }

    public function test_evaluation_empty_expression() : void
    {
        $this->assertNull(
            (new ScalarFunctions())->eval(Row::create(Entry::string('entry', 'value')))
        );
    }

    public function test_evaluation_equals_expression() : void
    {
        $this->assertTrue(
            (new ScalarFunctions(ref('entry')->equals(lit('1'))))
                ->eval(Row::create(Entry::string('entry', '1')))
        );
    }
}
