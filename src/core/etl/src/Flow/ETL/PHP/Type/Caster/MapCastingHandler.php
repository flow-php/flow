<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\{Caster, Type};

final class MapCastingHandler implements CastingHandler
{
    /**
     * @param Type<mixed> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof MapType;
    }

    /**
     * @param MapType $type
     */
    public function value(mixed $value, Type $type, Caster $caster, Options $options) : array
    {
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if (!\is_array($value)) {
                return [
                    $caster->to($type->key())->value(0) => $caster->to($type->value())->value($value),
                ];
            }

            $castedMap = [];

            foreach ($value as $key => $item) {
                $castedKey = $caster->to($type->key())->value($key);

                if ($castedKey === null) {
                    continue;
                }

                if (\array_key_exists($castedKey, $castedMap)) {
                    throw new CastingException($value, $type);
                }

                $castedMap[$caster->to($type->key())->value($key)] = $caster->to($type->value())->value($item);
            }

            return $castedMap;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type, $e);
        }
    }
}
