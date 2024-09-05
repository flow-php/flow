<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class NumberFormat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $value,
        private readonly ScalarFunction|int $decimals,
        private readonly ScalarFunction|string $decimalSeparator = '.',
        private readonly ScalarFunction|string $thousandsSeparator = ',',
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asNumber($row);
        $decimals = (new Parameter($this->decimals))->asInt($row);
        $decimalSeparator = (new Parameter($this->decimalSeparator))->asString($row);
        $thousandsSeparator = (new Parameter($this->thousandsSeparator))->asString($row);

        if ($value === null || $decimals === null || $decimalSeparator === null || $thousandsSeparator === null) {
            return null;
        }

        return \number_format((float) $value, $decimals, $decimalSeparator, $thousandsSeparator);
    }
}
