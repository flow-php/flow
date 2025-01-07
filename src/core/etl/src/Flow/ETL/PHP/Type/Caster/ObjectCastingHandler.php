<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_object;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\{Caster, Type};

final class ObjectCastingHandler implements CastingHandler
{
    /**
     * @param Type<object> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof ObjectType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : object
    {
        if (\is_object($value)) {
            return $value;
        }

        /** @var ObjectType $type */
        try {
            $object = (object) $value;

            if (!$object instanceof $type->class) {
                throw new CastingException($value, type_object($type->class));
            }

            return $object;
        } catch (\Throwable) {
            throw new CastingException($value, $type);
        }
    }
}
