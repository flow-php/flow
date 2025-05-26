<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\{Logical\OptionalType, TypeFactory, Types};

/**
 * @template TLeft
 * @template TRight
 *
 * @implements Type<TLeft&TRight>
 */
final readonly class IntersectionType implements Type
{
    private Types $flatTypes;

    /**
     * @param Type<TLeft> $left
     * @param Type<TRight> $right
     */
    public function __construct(private Type $left, private Type $right)
    {
        if ($left instanceof MixedType || $right instanceof MixedType) {
            throw new InvalidTypeException('IntersectionType cannot be mixed with MixedType, mixed is a standalone type');
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

        $this->flatTypes = \Flow\Types\DSL\types(...$types);
    }

    /**
     * @param array{type: 'intersection', left: array, right: array} $data
     *
     * @return type<TLeft&TRight>
     */
    public static function fromArray(array $data) : Type
    {
        return new self(
            TypeFactory::fromArray($data['left']),
            TypeFactory::fromArray($data['right']),
        );
    }

    /**
     * @return TLeft&TRight
     */
    public function assert(mixed $value) : mixed
    {
        if (!$this->isValid($value)) {
            throw InvalidTypeException::value($value, $this);
        }

        return $value;
    }

    public function cast(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            $leftCasted = $this->left->cast($value);

            if ($this->right->isValid($leftCasted)) {
                return $leftCasted;
            }
        } catch (CastingException) {
        }

        try {
            $rightCasted = $this->right->cast($value);

            if ($this->left->isValid($rightCasted)) {
                return $rightCasted;
            }
        } catch (CastingException) {
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->left->isValid($value) && $this->right->isValid($value);
    }

    /**
     * @return array{type: 'intersection', left: array, right: array}
     */
    public function normalize() : array
    {
        return [
            'type' => 'intersection',
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

        return 'intersection<' . \implode('&', $stringTypes) . '>';
    }

    public function types() : Types
    {
        return $this->flatTypes;
    }
}
