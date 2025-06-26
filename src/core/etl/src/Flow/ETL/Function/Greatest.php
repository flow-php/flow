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
        $extractedTypes = [];

        foreach ($this->values as $value) {
            $extractedValues[] = (new Parameter($value))->eval($row);
            $extractedTypes[] = (new Parameter($value))->asType($row);
        }

        if (!\count($extractedValues)) {
            return null;
        }

        (new ValueComparator())->assertAllTypesComparable($extractedTypes, '>');

        return max($extractedValues);
    }
}
