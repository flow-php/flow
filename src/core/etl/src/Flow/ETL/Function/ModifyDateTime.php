<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ModifyDateTime extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $reference,
        private readonly string|ScalarFunction $modifier,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->reference))->asInstanceOf($row, \DateTimeInterface::class);
        $modifier = (new Parameter($this->modifier))->asString($row);

        if ($modifier === null || $value === null) {
            return null;
        }

        if (!$value instanceof \DateTime && !$value instanceof \DateTimeImmutable) {
            return null;
        }

        return $value->modify($modifier);
    }
}
