<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_time;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Logical\TimeType, Type};

final class TimeCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof TimeType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : mixed
    {
        if ($value instanceof \DateTimeInterface) {
            return $value->diff(new \DateTimeImmutable($value->format('Y-m-d')), true);
        }

        if ($value instanceof \DateInterval) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (\is_string($value)) {
                return new \DateInterval($value);
            }
        } catch (\Throwable $e) {
            throw new CastingException($value, type_time());
        }

        throw new CastingException($value, $type);
    }
}
