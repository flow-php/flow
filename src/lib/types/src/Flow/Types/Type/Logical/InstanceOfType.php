<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_instance_of;
use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @template T of object
 *
 * @implements Type<T>
 */
final readonly class InstanceOfType implements Type
{
    /**
     * @param class-string<T> $class
     */
    public function __construct(public string $class)
    {
        if (!\class_exists($class) && !\interface_exists($class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @param array{class: class-string<T>} $data
     *
     * @return InstanceOfType<T>
     */
    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('class', $data)) {
            throw new InvalidArgumentException("Missing 'class' key in object type definition");
        }

        return new self($data['class']);
    }

    public function assert(mixed $value) : object
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : object
    {
        if (\is_object($value) && \is_a($value, $this->class, true)) {
            return $value;
        }

        try {
            $object = (object) $value;

            if (!$object instanceof $this->class) {
                throw new CastingException($value, type_instance_of($this->class));
            }

            return $object;
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_a($value, $this->class, true);
    }

    public function normalize() : array
    {
        return [
            'type' => 'instance_of',
            'class' => $this->class,
        ];
    }

    public function toString() : string
    {
        return 'object<' . $this->class . '>';
    }
}
