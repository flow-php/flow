<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class ObjectType implements NativeType
{
    /**
     * @param class-string $class
     */
    public function __construct(public readonly string $class)
    {
        if (!\class_exists($class) && !\interface_exists($this->class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param class-string $value
     */
    public static function fromString(string $value) : self
    {
        return new self($value);
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
