<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\TypeFactory;

use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * @template T
 *
 * @implements Type<T>
 */
final readonly class OptionalType implements Type
{
    /**
     * @var Type<T>
     */
    private Type $base;

    /**
     * Nullability is idempotent, so wrapping an already-optional type collapses to one level
     * instead of throwing - callers forcing nullability never need a guard.
     *
     * @param Type<T> $base
     *
     * @throws InvalidTypeException
     */
    public function __construct(Type $base)
    {
        if ($base instanceof MixedType) {
            throw new InvalidTypeException(
                'Optional type cannot be created from MixedType, mixed is a standalone type',
            );
        }

        if ($base instanceof UnionType) {
            throw new InvalidTypeException('Optional type cannot be created from a union type');
        }

        $this->base = $base instanceof self ? $base->base() : $base;
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return OptionalType<mixed>
     */
    public static function fromArray(array $data): self
    {
        type_structure([
            'type' => type_literal('optional'),
            'base' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(TypeFactory::fromArray($data['base']));
    }

    /**
     * @return T
     */
    public function assert(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->base->assert($value);
    }

    /**
     * @return Type<T>
     */
    public function base(): Type
    {
        return $this->base;
    }

    /**
     * @return T
     */
    public function cast(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return $this->base->cast($value);
    }

    public function isValid(mixed $value): bool
    {
        if ($value === null) {
            return true;
        }

        return $this->base->isValid($value);
    }

    /**
     * @return array{type: 'optional', base: array<string, mixed>}
     */
    public function normalize(): array
    {
        return [
            'type' => 'optional',
            'base' => $this->base->normalize(),
        ];
    }

    public function toString(): string
    {
        return '?' . $this->base->toString();
    }
}
