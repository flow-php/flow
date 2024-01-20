<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Structure;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class StructureElement
{
    public function __construct(private readonly string $name, private readonly Type $type)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Structure element name cannot be empty');
        }
    }

    public function isEqual(self $element) : bool
    {
        return $this->name === $element->name && $this->type->isEqual($element->type());
    }

    public function isValid(mixed $value) : bool
    {
        return $this->type->isValid($value);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function toString() : string
    {
        return $this->name . ': ' . $this->type->toString();
    }

    public function type() : Type
    {
        return $this->type;
    }
}
