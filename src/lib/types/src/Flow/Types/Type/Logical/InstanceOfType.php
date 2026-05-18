<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;

use function class_exists;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_structure;
use function interface_exists;
use function is_a;
use function is_object;
use function is_string;

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
    public function __construct(
        public string $class,
    ) {
        if (!class_exists($class) && !interface_exists($class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return InstanceOfType<object>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('instance_of'),
            'class' => type_class_string(),
        ])->assert($data);

        return new self($data['class']);
    }

    public function assert(mixed $value): object
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): object
    {
        if (is_object($value) && is_a($value, $this->class, true)) {
            return $value;
        }

        try {
            $object = (object) $value;

            if (!$object instanceof $this->class) {
                throw new CastingException($value, type_instance_of($this->class));
            }

            return $object;
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        return (is_object($value) || is_string($value)) && is_a($value, $this->class, true);
    }

    public function normalize(): array
    {
        return [
            'type' => 'instance_of',
            'class' => $this->class,
        ];
    }

    public function toString(): string
    {
        return 'object<' . $this->class . '>';
    }
}
