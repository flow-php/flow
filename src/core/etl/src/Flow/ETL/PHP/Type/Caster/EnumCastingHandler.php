<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\{Caster, Type};

final class EnumCastingHandler implements CastingHandler
{
    /**
     * @param Type<\UnitEnum> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof EnumType;
    }

    /**
     * @param EnumType $type
     */
    public function value(mixed $value, Type $type, Caster $caster, Options $options) : \UnitEnum
    {
        if ($value instanceof $type->class) {
            return $value;
        }

        try {
            /** @var EnumType $type */
            $enumClass = $type->class;

            if (\is_a($enumClass, \BackedEnum::class, true)) {
                return $enumClass::from($value);
            }

            throw new CastingException($value, $type);
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
