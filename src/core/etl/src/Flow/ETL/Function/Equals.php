<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_boolean;
use Flow\ETL\Function\Comparison\Comparable;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class Equals extends ScalarFunctionChain implements TypedScalarFunction
{
    use Comparable;

    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $left = (new Parameter($this->left))->eval($row);
        $right = (new Parameter($this->right))->eval($row);

        $this->assertComparable($left, $right, '==');

        return $left == $right;
    }

    public function returns() : Type
    {
        return type_boolean();
    }
}
