<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class ObjectType implements Type
{
    /**
     * @param class-string $class
     */
    public function __construct(public readonly string $class)
    {
        if (!\class_exists($class) && !\interface_exists($class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @param class-string $class
     */
    public static function of(string $class) : self
    {
        return new self($class);
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $type->class === $this->class;
    }

    public function isValid(mixed $value) : bool
    {
        return $value instanceof $this->class;
    }

    public function toString() : string
    {
        return 'object<' . $this->class . '>';
    }
}
