<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Row;

final class Trim implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly Type $type = Type::BOTH,
        private readonly string $characters = " \t\n\r\0\x0B"
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_string($value)) {
            return null;
        }

        $value = \strtolower($value);

        foreach (Type::cases() as $case) {
            if ($this->type->name === $case->name) {
                return ($case->value)($value, $this->characters);
            }
        }

        return null;
    }
}
