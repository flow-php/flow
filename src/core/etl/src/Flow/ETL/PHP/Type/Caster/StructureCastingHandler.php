<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\{Caster, Type};

final class StructureCastingHandler implements CastingHandler
{
    /**
     * @param Type<array> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof StructureType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : array
    {
        /** @var StructureType $type */
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            $castedStructure = [];

            foreach ($type->elements() as $elementName => $elementType) {

                $castedStructure[$elementName] = (\is_array($value) && \array_key_exists($elementName, $value))
                    ? $caster->to($elementType)->value($value[$elementName])
                    : $caster->to($elementType)->value(null);
            }

            return $castedStructure;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type, $e);
        }
    }
}
