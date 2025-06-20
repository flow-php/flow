<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

final class Greatest extends ScalarFunctionChain
{
    /**
     * @param array<mixed|ScalarFunction> $values
     */
    public function __construct(
        private readonly array $values,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $extractedValues = [];

        foreach ($this->values as $value) {
            $extractedValues[] = (new Parameter($value))->eval($row);
        }

        if (!\count($extractedValues)) {
            return null;
        }

        (new ValueComparator())->assertAllComparable($extractedValues, '>');

        return max($extractedValues);
    }
}
