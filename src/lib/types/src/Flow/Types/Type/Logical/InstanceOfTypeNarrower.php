<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{get_type, type_uuid};
use Flow\Types\Type;
use Flow\Types\Type\TypeNarrower;

final readonly class InstanceOfTypeNarrower implements TypeNarrower
{
    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value) : Type
    {
        if (!\is_object($value)) {
            return get_type($value);
        }

        $valueClass = $value::class;

        foreach (['Ramsey\Uuid\UuidInterface', 'Symfony\Component\Uid\Uuid'] as $uuidClass) {
            if (\is_a($valueClass, $uuidClass, true)) {
                return type_uuid();
            }
        }

        return get_type($value);
    }
}
