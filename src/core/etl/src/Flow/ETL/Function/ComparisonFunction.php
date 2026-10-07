<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

interface ComparisonFunction extends ScalarFunction
{
    /**
     * @return list<ScalarFunction> operands compared with each other, in children() order
     */
    public function operands(): array;

    /**
     * @param list<ScalarFunction> $operands same order as operands()
     */
    public function withOperands(array $operands): static;
}
