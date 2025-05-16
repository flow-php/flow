<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\{Logical\OptionalType, Type, TypeFactory, Types};

/**
 * @template TLeft
 * @template TRight
 *
 * @implements Type<TLeft|TRight>
 */
final readonly class UnionType implements Type
{
    private Types $flatTypes;

    /**
     * @param Type<TLeft> $left
     * @param Type<TRight> $right
     */
    public function __construct(private Type $left, private Type $right)
    {
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

        $this->flatTypes = \Flow\Types\DSL\types(...$types);
    }

    /**
     * @param array{type: 'union', left: array, right: array} $data
     *
     * @return type<TLeft|TRight>
     */
    public static function fromArray(array $data) : Type
    {
        return new self(
            TypeFactory::fromArray($data['left']),
            TypeFactory::fromArray($data['right']),
        );
    }

    /**
     * @return TLeft|TRight
     */
    public function assert(mixed $value) : mixed
    {
        if ($this->left->isValid($value)) {
            return $value;
        }

        if ($this->right->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : mixed
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

    public function isOptionalType() : bool
    {
        if (\count($this->types()) !== 2) {
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

    public function isValid(mixed $value) : bool
    {
        return $this->left->isValid($value) || $this->right->isValid($value);
    }

    /**
     * @return array{type: 'union', left: array, right: array}
     */
    public function normalize() : array
    {
        return [
            'type' => 'union',
            'left' => $this->left->normalize(),
            'right' => $this->right->normalize(),
        ];
    }

    public function toString() : string
    {
        $stringTypes = [];

        foreach ($this->flatTypes->deduplicate()->all() as $type) {
            if ($type instanceof OptionalType) {
                if (!\in_array($type->base()->toString(), $stringTypes, true)) {
                    $stringTypes[] = $type->base()->toString();
                }

                if (!\in_array('null', $stringTypes, true)) {
                    $stringTypes[] = 'null';
                }

                continue;
            }

            $stringTypes[] = $type->toString();
        }

        asort($stringTypes);

        return \implode('|', $stringTypes);
    }

    public function types() : Types
    {
        return $this->flatTypes;
    }
}
