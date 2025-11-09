<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class NumberFormat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $value,
        private readonly ScalarFunction|int $decimals,
        private readonly ScalarFunction|string $decimalSeparator = '.',
        private readonly ScalarFunction|string $thousandsSeparator = ',',
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asNumber($row, $context);
        $decimals = (new Parameter($this->decimals))->asInt($row, $context);
        $decimalSeparator = (new Parameter($this->decimalSeparator))->asString($row, $context);
        $thousandsSeparator = (new Parameter($this->thousandsSeparator))->asString($row, $context);

        if ($value === null || $decimals === null || $decimalSeparator === null || $thousandsSeparator === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('NumberFormat function requires non-null values'));
        }

        return \number_format((float) $value, $decimals, $decimalSeparator, $thousandsSeparator);
    }
}
