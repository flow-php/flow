<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{elements: array<Type>}>
 */
final class StructureType implements LogicalType, Serializable
{
    /**
     * @var array<Type>
     */
    private readonly array $elements;

    public function __construct(Type ...$types)
    {
        if (0 === \count($types)) {
            throw InvalidArgumentException::because('Structure must receive at least one element.');
        }

        $this->elements = $types;
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
                if (!$internalElement->isEqual($element)) {
                    return false;
                }
            }
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

    public function toString() : string
    {
        $content = [];

        foreach ($this->elements as $element) {
            $content[] = $element->toString();
        }

        return 'structure<' . \implode(', ', $content) . '>';
    }
}
