<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

/**
 * @implements NativeType<array{class: class-string, nullable: bool}>
 */
final class ObjectType implements NativeType
{
    /**
     * @param class-string $class
     */
    public function __construct(public readonly string $class, private readonly bool $nullable)
    {
        if (!\class_exists($class) && !\interface_exists($class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    public function __serialize() : array
    {
        return ['class' => $this->class, 'nullable' => $this->nullable];
    }

    public function __unserialize(array $data) : void
    {
        $this->class = $data['class'];
        $this->nullable = $data['nullable'];
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->class === $type->class && $this->nullable === $type->nullable;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_a($value, $this->class, true);
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'object<' . $this->class . '>';
    }
}
