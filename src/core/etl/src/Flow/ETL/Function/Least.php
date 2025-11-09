<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\Types\Type\ValueComparator;

final class Least extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed> $values
     */
    public function __construct(
        private readonly array $values,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $extractedValues = [];
        $extractedTypes = [];

        foreach ($this->values as $value) {
            $extractedValues[] = (new Parameter($value))->eval($row, $context);
            $extractedTypes[] = (new Parameter($value))->asType($row, $context);
        }

        if (!\count($extractedValues)) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Least requires at least one value'));
        }

        (new ValueComparator())->assertAllTypesComparable($extractedTypes, '<');

        return \min($extractedValues);
    }
}
