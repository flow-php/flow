<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Type;

final class ListCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ListType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        if (\is_array($value)) {
            return $value;
        }

        try {
            if (\is_string($value)) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            return (array) $value;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
