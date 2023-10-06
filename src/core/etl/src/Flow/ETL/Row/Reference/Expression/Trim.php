<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Trim implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly Expression\Trim\Type $type = Expression\Trim\Type::BOTH,
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

        foreach (Expression\Trim\Type::cases() as $case) {
            if ($this->type->name === $case->name) {
                return ($case->value)($value, $this->characters);
            }
        }

        throw new InvalidArgumentException("Unsupported trim method: {$value}");
    }
}
