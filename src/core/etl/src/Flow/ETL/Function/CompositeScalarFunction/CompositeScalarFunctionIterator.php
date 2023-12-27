<?php declare(strict_types=1);

namespace Flow\ETL\Function\CompositeScalarFunction;

use Flow\ETL\Function\CompositeScalarFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;

/**
 * @template-implements \IteratorAggregate<ScalarFunction>
 */
final class CompositeScalarFunctionIterator implements \IteratorAggregate
{
    public function __construct(private readonly CompositeScalarFunction $function)
    {
    }

    public function getIterator() : \Generator
    {
        foreach ($this->function->functions() as $function) {
            if ($function instanceof CompositeScalarFunction) {
                foreach ((new self($function))->getIterator() as $subFunction) {
                    yield $subFunction;
                }
            } elseif ($function instanceof ScalarFunctionChain) {
                yield $function->getRootFunction();
            }
        }
    }
}
