<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_json;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\JsonType;
use Flow\ETL\PHP\Type\Type;

final class JsonCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof JsonType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        try {
            if (\is_string($value)) {
                return \json_encode(\json_decode($value, true, 512, \JSON_THROW_ON_ERROR), JSON_THROW_ON_ERROR);
            }

            if (\is_scalar($value)) {
                throw new CastingException($value, type_json());
            }

            return \json_encode($value, JSON_THROW_ON_ERROR);
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
