<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class ArrayFilter extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed> $array
     */
    public function __construct(private readonly ScalarFunction|array $array, private readonly mixed $value = null)
    {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if (null === $array) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ArrayFilter function requires non-null array'));
        }

        $value = (new Parameter($this->value))->eval($row, $context);

        return \array_filter($array, static fn ($item) => $item !== $value);
    }
}
