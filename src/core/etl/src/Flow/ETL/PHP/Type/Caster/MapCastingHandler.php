<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Type;

final class MapCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof MapType;
    }

    public function value(mixed $value, Type $type, Caster $caster) : mixed
    {
        /** @var MapType $type */
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if (!\is_array($value)) {
                return [
                    $caster->to($type->key()->type())->value(0) => $caster->to($type->value()->type())->value($value),
                ];
            }

            $castedMap = [];

            foreach ($value as $key => $item) {
                $castedKey = $caster->to($type->key()->type())->value($key);

                if (\array_key_exists($castedKey, $castedMap)) {
                    throw new CastingException($value, $type);
                }

                $castedMap[$caster->to($type->key()->type())->value($key)] = $caster->to($type->value()->type())->value($item);
            }

            return $castedMap;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type, $e);
        }
    }
}
