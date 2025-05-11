<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\PHP\Type\ValueComparator;
use Flow\ETL\Row;

final class Greatest extends ScalarFunctionChain
{
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
