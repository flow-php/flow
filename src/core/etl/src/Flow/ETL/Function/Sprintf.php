<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Sprintf extends ScalarFunctionChain
{
    /**
     * @var array<null|float|int|ScalarFunction|string>
     */
    private readonly array $values;

    public function __construct(
        private readonly ScalarFunction|string $format,
        ScalarFunction|float|int|string|null ...$values,
    ) {
        $this->values = $values;
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $format = (new Parameter($this->format))->asString($row, $context);

        /**
         * @var array<null|float|int|string> $values
         */
        $values = \array_map(static fn (ScalarFunction|float|int|string|null $value) : mixed => (new Parameter($value))->eval($row, $context), $this->values);

        if ($format === null || \in_array(null, $values, true)) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Sprintf requires non-null format and values'));
        }

        /** @var array<float|int|string> $nonNullValues */
        $nonNullValues = $values;

        return \sprintf($format, ...$nonNullValues);
    }
}
