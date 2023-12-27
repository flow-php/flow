<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function\CompositeScalarFunction;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Function\CompositeScalarFunction\CompositeScalarFunctionIterator;
use PHPUnit\Framework\TestCase;

final class CompositeScalarFunctionIteratorTest extends TestCase
{
    public function test_iterating_over_functions() : void
    {
        $function = all(
            ref('year')->greaterThanEqual(lit(2020)),
            ref('month')->greaterThanEqual(lit(5)),
        );

        $iterator = new CompositeScalarFunctionIterator($function);

        $this->assertEquals(
            [
                ref('year'),
                ref('month'),
            ],
            \iterator_to_array($iterator->getIterator())
        );
    }

    public function test_iterating_over_nested_composite_functions() : void
    {
        $function = all(
            all(
                ref('year')->greaterThanEqual(lit(2020)),
                ref('month')->greaterThanEqual(lit(5)),
            ),
            any(
                ref('group')->equals(lit('A')),
                ref('id')->greaterThan(lit(100)),
                any(
                    ref('age')->greaterThanEqual(lit(20))
                )
            )
        );

        $iterator = new CompositeScalarFunctionIterator($function);

        $this->assertEquals(
            [
                ref('year'),
                ref('month'),
                ref('group'),
                ref('id'),
                ref('age'),
            ],
            \iterator_to_array($iterator->getIterator())
        );
    }
}
