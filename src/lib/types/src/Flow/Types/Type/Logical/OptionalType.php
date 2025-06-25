<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_literal, type_map, type_mixed, type_string, type_structure};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\{Native\MixedType, TypeFactory};
use Flow\Types\Type\Native\UnionType;

/**
 * @template T
 *
 * @implements Type<T>
 */
final readonly class OptionalType implements Type
{
    /**
     * @param Type<T> $base
     *
     * @throws InvalidTypeException
     */
    public function __construct(private Type $base)
    {
        if ($base instanceof MixedType) {
            throw new InvalidTypeException('Optional type cannot be created from MixedType, mixed is a standalone type');
        }

        if ($base instanceof UnionType) {
            throw new InvalidTypeException('Optional type cannot be created from a union type');
        }

        if ($base instanceof self) {
            throw new InvalidTypeException('Optional type cannot be created from an optional type');
        }
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return OptionalType<mixed>
     */
    public static function fromArray(array $data) : self
    {
        type_structure([
            'type' => type_literal('optional'),
            'base' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(TypeFactory::fromArray($data['base']));
    }

    public function assert(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->base->assert($value);
    }

    /**
     * @return Type<T>
     */
    public function base() : Type
    {
        return $this->base;
    }

    public function cast(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->base->cast($value);
    }

    public function isValid(mixed $value) : bool
    {
        if ($value === null) {
            return true;
        }

        return $this->base->isValid($value);
    }

    /**
     * @return array{type: 'optional', base: array<string, mixed>}
     */
    public function normalize() : array
    {
        return [
            'type' => 'optional',
            'base' => $this->base->normalize(),
        ];
    }

    public function toString() : string
    {
        return '?' . $this->base->toString();
    }
}
