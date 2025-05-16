<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_string, type_union};
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\Type;

/**
 * @implements Type<int|float|string|bool>
 */
final readonly class ScalarType implements Type
{
    /**
     * @var UnionType<bool|float|int|string,bool|float|int|string>
     */
    private UnionType $innerType;

    public function __construct()
    {
        $this->innerType = type_union(
            type_string(),
            type_integer(),
            type_boolean(),
            type_float()
        );
    }

    public function assert(mixed $value) : int|float|string|bool
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
