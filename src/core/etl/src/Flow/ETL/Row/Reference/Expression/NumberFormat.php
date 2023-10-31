<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class NumberFormat implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly Expression $decimals,
        private readonly Expression $decimalSeparator,
        private readonly Expression $thousandsSeparator
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
