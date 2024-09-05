<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Row;

final class Trim extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|Type $type = Type::BOTH,
        private readonly ScalarFunction|string $characters = " \t\n\r\0\x0B",
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->asString($row);
        $type = (new Parameter($this->type))->asEnum($row, Type::class);
        $characters = (new Parameter($this->characters))->asString($row);

        if ($value === null || $type === null || $characters === null) {
            return null;
        }

        foreach (Type::cases() as $case) {
            if ($type->name === $case->name) {
                return ($case->value)($value, $characters);
            }
        }

        return null;
    }
}
