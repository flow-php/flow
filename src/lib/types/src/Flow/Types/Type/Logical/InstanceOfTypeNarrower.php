<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_uuid;
use Flow\Types\Type;
use Flow\Types\Type\TypeNarrower;

final readonly class InstanceOfTypeNarrower implements TypeNarrower
{
    /**
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public static function narrow(Type $type, mixed $value) : Type
    {
        if (!$type instanceof InstanceOfType) {
            return $type;
        }

        foreach (['Ramsey\Uuid\UuidInterface', 'Symfony\Component\Uid\Uuid'] as $uuidClass) {
            if (\is_a($type->class, $uuidClass, true)) {
                return type_uuid();
            }
        }

        return $type;
    }
}
