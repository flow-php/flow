<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Row;

final class Between extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly mixed $lowerBoundRef,
        private readonly mixed $upperBoundRef,
        private readonly ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->eval($row);
        $lowerBound = (new Parameter($this->lowerBoundRef))->eval($row);
        $upperBound = (new Parameter($this->upperBoundRef))->eval($row);
        $boundary = (new Parameter($this->boundary))->asEnum($row, Boundary::class);

        if (!$boundary instanceof Boundary) {
            return null;
        }

        return $boundary->compare($value, $lowerBound, $upperBound);
    }
}
