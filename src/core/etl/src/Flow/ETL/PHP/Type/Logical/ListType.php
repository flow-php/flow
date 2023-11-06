<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Type;

final class ListType implements Type
{
    public function __construct(private readonly List\ListElement $element)
    {
    }

    public function element() : ListElement
    {
        return $this->element;
    }

    public function isEqual(Type $type) : bool
    {
        if (!$type instanceof self) {
            return false;
        }

        if ($this->element->toString() !== $type->element()->toString()) {
            return false;
        }

        return true;
    }

    public function isValid(mixed $value) : bool
    {
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

    public function toString() : string
    {
        return $this->element->toString();
    }
}
