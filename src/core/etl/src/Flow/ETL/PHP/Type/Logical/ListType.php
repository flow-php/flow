<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\{Type, TypeFactory};

/**
 * @implements Type<array<int, mixed>>
 */
final readonly class ListType implements Type
{
    /**
     * @param Type<mixed> $element
     * @param bool $nullable
     */
    public function __construct(private Type $element, private bool $nullable = false)
    {
    }

    public static function fromArray(array $data) : self
    {
        return new self(TypeFactory::fromArray($data['element']), $data['nullable'] ?? false);
    }

    /**
     * @return Type<mixed>
     */
    public function element() : Type
    {
        return $this->element;
    }

    public function isComparableWith(Type $type) : bool
    {
        if ($type instanceof self) {
            return true;
        }

        if ($type instanceof NullType) {
            return true;
        }

        return false;
    }

    public function isEqual(Type $type) : bool
    {
        if (!$type instanceof self) {
            return false;
        }

        return $this->element->toString() === $type->element()->toString();
    }

    public function isValid(mixed $value) : bool
    {
        if ($this->nullable && $value === null) {
            return true;
        }

        if (!\is_array($value)) {
            return false;
        }

        if ([] !== $value && !\array_is_list($value)) {
            return false;
        }

        foreach ($value as $item) {
            if (!$this->element->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->element, $nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        return new self($this->element, $this->nullable || $type->nullable());
    }

    public function normalize() : array
    {
        return [
            'type' => 'list',
            'element' => $this->element->normalize(),
            'nullable' => $this->nullable,
        ];
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'list<' . $this->element->toString() . '>';
    }
}
