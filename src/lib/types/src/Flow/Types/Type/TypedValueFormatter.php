<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use BackedEnum;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\UnionType;
use UnitEnum;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_string;
use function number_format;

final readonly class TypedValueFormatter
{
    /**
     * @param Type<mixed> $type
     */
    public function format(Type $type, mixed $value): string
    {
        if ($value === null) {
            return '';
        }

        if ($type instanceof UnionType) {
            $member = $type->memberFor($value);

            return $member === null ? type_string()->cast($value) : $this->format($member, $value);
        }

        return match (true) {
            $type instanceof FloatType => number_format(type_float()->assert($value), 6, '.', ''),
            $type instanceof DateType => type_date()->assert($value)->format('Y-m-d'),
            $type instanceof EnumType && $value instanceof BackedEnum => (string) $value->value,
            $type instanceof EnumType && $value instanceof UnitEnum => $value->name,
            default => type_string()->cast($value),
        };
    }
}
