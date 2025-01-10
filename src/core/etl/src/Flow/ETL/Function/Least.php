<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Comparison\Comparable;
use Flow\ETL\Row;

final class Least extends ScalarFunctionChain
{
    use Comparable;

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

        $this->assertAllComparable($extractedValues, '<');

        return \min($extractedValues);
    }
}
