<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\{Type, TypeFactory};

/**
 * @implements Type<array>
 */
final readonly class StructureType implements Type
{
    /**
     * @var array<string, Type<mixed>>
     */
    private array $elements;

    /**
     * @param array<string, Type<mixed>> $elements
     * @param bool $nullable
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $elements, private bool $nullable = false)
    {
        if (0 === \count($elements)) {
            throw InvalidArgumentException::because('Structure must receive at least one element.');
        }

        foreach ($elements as $name => $type) {
            //            if (!\is_string($name)) {
            //                throw InvalidArgumentException::because('Structure element name must be a string');
            //            }

            if (!$type instanceof Type) {
                throw InvalidArgumentException::because('Structure element type must be an instance of Type');
            }
        }

        $this->elements = $elements;
    }

    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('elements', $data)) {
            throw InvalidArgumentException::because('Structure must receive at least one element.');
        }

        $elements = [];

        foreach ($data['elements'] as $name => $element) {
            $elements[$name] = TypeFactory::fromArray($element);
        }

        return new self($elements, $data['nullable'] ?? false);
    }

    /**
     * @return array<string, Type<mixed>>
     */
    public function elements() : array
    {
        return $this->elements;
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

        if (\count($this->elements) !== \count($type->elements())) {
            return false;
        }

        foreach ($this->elements as $internalElementName => $internalElement) {
            foreach ($type->elements as $elementName => $element) {
                if ($elementName === $internalElementName && $element->isEqual($internalElement)) {
                    continue 2;
                }
            }

            return false;
        }

        return true;
    }

    public function isValid(mixed $value) : bool
    {
        if ($this->nullable && $value === null) {
            return true;
        }

        if (!\is_array($value)) {
            return false;
        }

        if (\array_is_list($value)) {
            return false;
        }

        foreach ($value as $itemName => $item) {
            foreach ($this->elements as $name => $element) {
                if ($itemName === $name && $element->isValid($item)) {
                    continue 2;
                }
            }

            return false;
        }

        return true;
    }

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->elements, $nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw InvalidArgumentException::because('Cannot merge "%s" with "%s"', $this->toString(), $type->toString());
        }

        $elements = [];

        foreach ($this->elements as $name => $thisElement) {
            $elements[$name] = $thisElement;
        }

        $typeElements = [];

        foreach ($type->elements() as $name => $typeElement) {
            $typeElements[$name] = $typeElement;
        }

        foreach ($type->elements as $name => $structElement) {
            if (\array_key_exists($name, $elements)) {
                $elements[$name] = $elements[$name]->merge($structElement);
            } else {
                $elements[$name] = $structElement->makeNullable(true);
            }
        }

        foreach ($this->elements as $name => $thisElement) {
            if (!\array_key_exists($name, $typeElements)) {
                $elements[$name] = $thisElement->makeNullable(true);
            }
        }

        return new self($elements, $this->nullable || $type->nullable());
    }

    public function normalize() : array
    {
        $elements = [];

        foreach ($this->elements as $name => $element) {
            $elements[$name] = $element->normalize();
        }

        return [
            'type' => 'structure',
            'elements' => $elements,
            'nullable' => $this->nullable,
        ];
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        $content = [];

        foreach ($this->elements as $name => $element) {
            $content[] = $name . ': ' . $element->toString();
        }

        return ($this->nullable ? '?' : '') . 'structure{' . \implode(', ', $content) . '}';
    }
}
