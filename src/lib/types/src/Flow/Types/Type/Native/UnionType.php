<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Type\Types;

use function count;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\types;
use function implode;
use function in_array;

/**
 * @template TLeft
 * @template TRight
 *
 * @implements Type<TLeft|TRight>
 */
final readonly class UnionType implements Type
{
    /**
     * @var Types<TLeft|TRight>
     */
    private Types $flatTypes;

    /**
     * @param Type<TLeft> $left
     * @param Type<TRight> $right
     */
    public function __construct(
        private Type $left,
        private Type $right,
    ) {
        if ($left instanceof MixedType || $right instanceof MixedType) {
            throw new InvalidTypeException('UnionType cannot be mixed with MixedType, mixed is a standalone type');
        }

        $types = [];

        if ($this->left instanceof self) {
            $types = [...$types, ...$this->left->types()->all()];
        } else {
            $types[] = $this->left;
        }

        if ($this->right instanceof self) {
            $types = [...$types, ...$this->right->types()->all()];
        } else {
            $types[] = $this->right;
        }

        $this->flatTypes = types(...$types);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return UnionType<mixed, mixed>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('union'),
            'left' => type_map(type_string(), type_mixed()),
            'right' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(TypeFactory::fromArray($data['left']), TypeFactory::fromArray($data['right']));
    }

    /**
     * @return TLeft|TRight
     */
    public function assert(mixed $value): mixed
    {
        if ($this->left->isValid($value)) {
            return $value;
        }

        if ($this->right->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return TLeft|TRight
     */
    public function cast(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            return $this->left->cast($value);
        } catch (CastingException) {
            // ignore
        }

        try {
            return $this->right->cast($value);
        } catch (CastingException) {
            // ignore
        }

        throw new CastingException($value, $this);
    }

    public function isOptionalType(): bool
    {
        if (count($this->types()) !== 2) {
            return false;
        }

        if ($this->types()->deduplicate()->count() === 1) {
            return false;
        }

        foreach ($this->types()->all() as $nextType) {
            if ($nextType instanceof NullType) {
                return true;
            }
        }

        return false;
    }

    public function isValid(mixed $value): bool
    {
        return $this->left->isValid($value) || $this->right->isValid($value);
    }

    /**
     * @return null|Type<mixed> the first non-null member this value is valid for
     */
    public function memberFor(mixed $value): ?Type
    {
        foreach ($this->types()->all() as $member) {
            if ($member instanceof OptionalType) {
                $member = $member->base();
            }

            if ($member instanceof NullType) {
                continue;
            }

            if ($member->isValid($value)) {
                return $member;
            }
        }

        return null;
    }

    /**
     * @return array{type: 'union', left: array<string, mixed>, right: array<string, mixed>}
     */
    public function normalize(): array
    {
        return [
            'type' => 'union',
            'left' => $this->left->normalize(),
            'right' => $this->right->normalize(),
        ];
    }

    public function toString(): string
    {
        $stringTypes = [];

        foreach ($this->flatTypes->deduplicate()->all() as $type) {
            if ($type instanceof OptionalType) {
                if (!in_array($type->base()->toString(), $stringTypes, true)) {
                    $stringTypes[] = $type->base()->toString();
                }

                if (!in_array('null', $stringTypes, true)) {
                    $stringTypes[] = 'null';
                }

                continue;
            }

            $stringTypes[] = $type->toString();
        }

        asort($stringTypes);

        return implode('|', $stringTypes);
    }

    /**
     * @return Types<TLeft|TRight>
     */
    public function types(): Types
    {
        return $this->flatTypes;
    }
}
