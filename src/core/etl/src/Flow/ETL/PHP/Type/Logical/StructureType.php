<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Type;

/**
 * @implements LogicalType<array{elements: array<StructureElement>}>
 */
final class StructureType implements LogicalType
{
    /**
     * @var array<StructureElement>
     */
    private readonly array $elements;

    public function __construct(StructureElement ...$elements)
    {
        if (0 === \count($elements)) {
            throw InvalidArgumentException::because('Structure must receive at least one element.');
        }

        if (\count($elements) !== \count(\array_unique(\array_map(fn (StructureElement $element) => $element->name(), $elements)))) {
            throw InvalidArgumentException::because('All structure element names must be unique');
        }

        $this->elements = $elements;
    }

    public function __serialize() : array
    {
        return ['elements' => $this->elements];
    }

    public function __unserialize(array $data) : void
    {
        $this->elements = $data['elements'];
    }

    public function elements() : array
    {
        return $this->elements;
    }

    public function isEqual(Type $type) : bool
    {
        if (!$type instanceof self) {
            return false;
        }

        foreach ($this->elements as $internalElement) {
            foreach ($type->elements() as $element) {
                if ($internalElement->name() === $element->name() && $internalElement->isEqual($element->type())) {
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

    public function nullable() : bool
    {
        return false;
    }

    public function toString() : string
    {
        $content = [];

        foreach ($this->elements as $element) {
            $content[] = $element->toString();
        }

        return 'structure{' . \implode(', ', $content) . '}';
    }
}
