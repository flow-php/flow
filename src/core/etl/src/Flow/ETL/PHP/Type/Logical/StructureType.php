<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Type;

final class StructureType implements LogicalType
{
    /**
     * @var array<StructureElement>
     */
    private readonly array $elements;

    public function __construct(array $elements, private readonly bool $nullable = false)
    {
        if (0 === \count($elements)) {
            throw InvalidArgumentException::because('Structure must receive at least one element.');
        }

        if (\count($elements) !== \count(\array_unique(\array_map(fn (StructureElement $element) => $element->name(), $elements)))) {
            throw InvalidArgumentException::because('All structure element names must be unique');
        }

        $this->elements = $elements;
    }

    /**
     * @return array<StructureElement>
     */
    public function elements() : array
    {
        return $this->elements;
    }

    public function isEqual(Type $type) : bool
    {
        if (!$type instanceof self) {
            return false;
        }

        if (\count($this->elements) !== \count($type->elements())) {
            return false;
        }

        foreach ($this->elements as $internalElement) {
            foreach ($type->elements as $element) {
                if ($element->isEqual($internalElement)) {
                    continue 2;
                }
            }

            return false;
        }

        return true;
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        if (\array_is_list($value)) {
            return false;
        }

        foreach ($value as $item) {
            foreach ($this->elements as $element) {
                if ($element->isValid($item)) {
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

        foreach ($this->elements as $thisElement) {
            $elements[$thisElement->name()] = $thisElement;
        }

        $typeElements = [];

        foreach ($type->elements() as $typeElement) {
            $typeElements[$typeElement->name()] = $typeElement;
        }

        foreach ($type->elements as $structElement) {
            if (\array_key_exists($structElement->name(), $elements)) {
                $elements[$structElement->name()] = $elements[$structElement->name()]->merge($structElement);
            } else {
                $elements[$structElement->name()] = $structElement->makeNullable(true);
            }
        }

        foreach ($this->elements as $thisElement) {
            if (!\array_key_exists($thisElement->name(), $typeElements)) {
                $elements[$thisElement->name()] = $thisElement->makeNullable(true);
            }
        }

        return new self(\array_values($elements), $this->nullable || $type->nullable());
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        $content = [];

        foreach ($this->elements as $element) {
            $content[] = $element->toString();
        }

        return ($this->nullable ? '?' : '') . 'structure{' . \implode(', ', $content) . '}';
    }
}
