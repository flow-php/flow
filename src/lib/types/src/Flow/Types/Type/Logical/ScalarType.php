<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_string, type_union};
use Flow\Types\Type;

/**
 * @implements Type<bool|float|int|string>
 */
final readonly class ScalarType implements Type
{
    /**
     * @var Type<bool|float|int|string>
     */
    private Type $innerType;

    public function __construct()
    {
        $this->innerType = type_union(type_string(), type_integer(), type_boolean(), type_float());
    }

    #[\Override]
    public function assert(mixed $value): string|int|bool|float
    {
        return $this->innerType->assert($value);
    }

    #[\Override]
    public function cast(mixed $value): int|float|string|bool
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->innerType->cast($value);
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return $this->innerType->isValid($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'scalar',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'scalar';
    }
}
