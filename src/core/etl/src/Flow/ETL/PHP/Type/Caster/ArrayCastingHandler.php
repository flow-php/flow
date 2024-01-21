<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Type;

final class ArrayCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ArrayType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        try {
            if (\is_string($value)) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if (\is_array($value)) {
                return $value;
            }

            if (\is_object($value)) {
                return \json_decode(\json_encode($value, JSON_THROW_ON_ERROR), true, 512, \JSON_THROW_ON_ERROR);
            }

            return (array) $value;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
