<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_string, type_union};
use Flow\Types\Type;

/**
 * @implements Type<string|int|bool|float>
 */
final readonly class ScalarType implements Type
{
    /**
     * @var Type<bool|float|int|string>
     */
    private Type $innerType;

    public function __construct()
    {
        $this->innerType = type_union(
            type_string(),
            type_integer(),
            type_boolean(),
            type_float()
        );
    }

    public function assert(mixed $value) : string|int|bool|float
    {
        return $this->innerType->assert($value);
    }

    public function cast(mixed $value) : int|float|string|bool
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->innerType->cast($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->innerType->isValid($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'scalar',
        ];
    }

    public function toString() : string
    {
        return 'scalar';
    }
}
