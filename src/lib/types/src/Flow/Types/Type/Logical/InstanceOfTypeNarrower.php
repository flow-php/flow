<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Type;
use Flow\Types\Type\TypeDetector;
use Flow\Types\Type\TypeNarrower;

use function Flow\Types\DSL\type_uuid;
use function is_a;
use function is_object;

final readonly class InstanceOfTypeNarrower implements TypeNarrower
{
    public function __construct(
        private TypeDetector $detector = new TypeDetector(),
    ) {}

    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value): Type
    {
        if (!is_object($value)) {
            return $this->detector->detectType($value);
        }

        $valueClass = $value::class;

        foreach (['Ramsey\Uuid\UuidInterface', 'Symfony\Component\Uid\Uuid'] as $uuidClass) {
            if (is_a($valueClass, $uuidClass, true)) {
                return type_uuid();
            }
        }

        return $this->detector->detectType($value);
    }
}
