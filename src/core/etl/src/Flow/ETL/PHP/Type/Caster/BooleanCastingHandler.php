<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Native\BooleanType, Type};

final class BooleanCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof BooleanType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : mixed
    {
        if (\is_bool($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        if (\is_string($value)) {
            if (\in_array(\mb_strtolower($value), ['true', '1', 'yes', 'on'], true)) {
                return true;
            }

            if (\in_array(\mb_strtolower($value), ['false', '0', 'no', 'off'], true)) {
                return false;
            }
        }

        try {
            return (bool) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
