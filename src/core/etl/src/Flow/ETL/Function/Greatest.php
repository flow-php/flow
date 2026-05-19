<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

use function count;

final class Greatest extends ScalarFunctionChain
{
    /**
     * @param array<mixed|ScalarFunction> $values
     */
    public function __construct(
        private readonly array $values,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $extractedValues = [];
        $extractedTypes = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values as $value) {
            $extractedValues[] = (new Parameter($value))->eval($row, $context);
            $extractedTypes[] = (new Parameter($value))->asType($row, $context);
        }

        if (!count($extractedValues)) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Greatest requires at least one value'));
        }

        (new ValueComparator())->assertAllTypesComparable($extractedTypes, '>');

        return max($extractedValues);
    }
}
