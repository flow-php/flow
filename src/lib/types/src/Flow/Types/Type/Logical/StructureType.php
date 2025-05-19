<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type\{Type, TypeFactory};

/**
 * @template T of array
 *
 * @implements Type<T>
 */
final readonly class StructureType implements Type
{
    /**
     * @var T
     */
    private array $elements;

    /**
     * @param T $elements
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $elements)
    {
        if (0 === \count($elements)) {
            throw new InvalidArgumentException('Structure must receive at least one element.');
        }

        foreach ($elements as $type) {
            if (!$type instanceof Type) {
                throw new InvalidArgumentException('Structure element type must be an instance of Type');
            }
        }

        $this->elements = $elements;
    }

    /**
     * @param array{type: 'structure', elements: array} $data
     *
     * @return StructureType<array<Type<mixed>>>
     */
    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('elements', $data)) {
            throw new InvalidArgumentException('Structure must receive at least one element.');
        }

        $elements = [];

        foreach ($data['elements'] as $name => $element) {
            $elements[$name] = TypeFactory::fromArray($element);
        }

        return new self($elements);
    }

    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return $this->assert(\json_decode($value, true, 512, \JSON_THROW_ON_ERROR));
            }

            $castedStructure = [];

            foreach ($this->elements as $elementName => $elementType) {
                $castedStructure[$elementName] = (\is_array($value) && \array_key_exists($elementName, $value))
                    ? $elementType->cast($value[$elementName])
                    : $elementType->cast(null);
            }

            return $this->assert($castedStructure);
        } catch (\Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    /**
     * @return array<string, Type<mixed>>
     */
    public function elements() : array
    {
        return $this->elements;
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        if (\array_is_list($value)) {
            return false;
        }

        if (\count($value) !== \count($this->elements)) {
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

    /**
     * @return array{type: 'structure', elements: array}
     */
    public function normalize() : array
    {
        $elements = [];

        foreach ($this->elements as $name => $element) {
            $elements[$name] = $element->normalize();
        }

        return [
            'type' => 'structure',
            'elements' => $elements,
        ];
    }

    public function toString() : string
    {
        $content = [];

        foreach ($this->elements as $name => $element) {
            $content[] = $name . ': ' . $element->toString();
        }

        return 'structure{' . \implode(', ', $content) . '}';
    }
}
