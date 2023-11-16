<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class NumberFormat implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction $decimals,
        private readonly ScalarFunction $decimalSeparator,
        private readonly ScalarFunction $thousandsSeparator
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = $this->ref->eval($row);
        $decimals = $this->decimals->eval($row);
        $decimalSeparator = $this->decimalSeparator->eval($row);
        $thousandsSeparator = $this->thousandsSeparator->eval($row);

        if (!\is_numeric($value)) {
            return null;
        }

        if (!\is_int($decimals)) {
            return null;
        }

        return \number_format((float) $value, $decimals, $decimalSeparator, $thousandsSeparator);
    }
}
