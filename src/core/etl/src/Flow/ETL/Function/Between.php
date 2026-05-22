<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Row;

final class Between extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly mixed $lowerBoundRef,
        private readonly mixed $upperBoundRef,
        private readonly ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $boundary = (new Parameter($this->boundary))->asEnum($row, $context, Boundary::class);

        if (!$boundary instanceof Boundary) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Between function requires valid boundary'));
        }

        return $boundary->compare(
            (new Parameter($this->value))->eval($row, $context),
            (new Parameter($this->lowerBoundRef))->eval($row, $context),
            (new Parameter($this->upperBoundRef))->eval($row, $context),
        );
    }
}
