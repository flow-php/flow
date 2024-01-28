<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Structure;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\PHP\Type\TypeFactory;

final class StructureElement
{
    public function __construct(private readonly string $name, private readonly Type $type)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Structure element name cannot be empty');
        }
    }

    public static function fromArray(array $element) : self
    {
        if (!\array_key_exists('name', $element)) {
            throw InvalidArgumentException::because('Structure element must have a name');
        }

        if (!\array_key_exists('type', $element)) {
            throw InvalidArgumentException::because('Structure element must have a type');
        }

        return new self($element['name'], TypeFactory::fromArray($element['type']));
    }

    public function isEqual(self $element) : bool
    {
        return $this->name === $element->name && $this->type->isEqual($element->type());
    }

    public function isValid(mixed $value) : bool
    {
        return $this->type->isValid($value);
    }

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->name, $this->type->makeNullable($nullable));
    }

    public function merge(self $element) : self
    {
        if ($this->name !== $element->name) {
            throw InvalidArgumentException::because('Cannot merge structure elements with different names');
        }

        return new self($this->name, $this->type->merge($element->type()));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'type' => $this->type->normalize(),
        ];
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
