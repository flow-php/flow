<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @template T of object
 *
 * @implements Type<class-string<T>>
 */
final readonly class ClassStringType implements Type
{
    /**
     * @param null|class-string<T> $class
     */
    public function __construct(public ?string $class = null)
    {
        if ($class !== null && (!\class_exists($class) && !\interface_exists($class))) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @param array{class?: class-string<T>} $data
     *
     * @return Type<class-string<T>>
     */
    public static function fromArray(array $data) : Type
    {
        return new self($data['class'] ?? null);
    }

    public function assert(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value)) {
            if (\class_exists($value) || \interface_exists($value)) {
                if ($this->class === null || \is_a($value, $this->class, true)) {
                    /** @phpstan-ignore-next-line */
                    return $value;
                }
            }
        }

        if (\is_object($value)) {
            $className = $value::class;

            if ($this->class === null || \is_a($className, $this->class, true)) {
                /** @phpstan-ignore-next-line */
                return $className;
            }
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_string($value)) {
            return false;
        }

        if (!\class_exists($value) && !\interface_exists($value)) {
            return false;
        }

        if ($this->class === null) {
            return true;
        }

        return \is_a($value, $this->class, true);
    }

    public function normalize() : array
    {
        $result = ['type' => 'class_string'];

        if ($this->class !== null) {
            $result['class'] = $this->class;
        }

        return $result;
    }

    public function toString() : string
    {
        return $this->class === null ? 'class-string' : 'class-string<' . $this->class . '>';
    }
}
