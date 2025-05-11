<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use function Flow\ETL\DSL\type_object;
use _PHPStan_95d365e52\Nette\PhpGenerator\ClassType;
use Flow\ETL\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\ETL\PHP\Type\Type;

/**
 * @template ClassType
 *
 * @implements Type<object<ClassType>>
 */
final readonly class ObjectType implements Type
{
    /**
     * @param class-string<ClassType> $class
     */
    public function __construct(public string $class)
    {
        if (!\class_exists($class) && !\interface_exists($class)) {
            throw new InvalidArgumentException("Class {$class} not found");
        }
    }

    /**
     * @param array{class: class-string<ClassType>} $data
     *
     * @return ObjectType<ClassType>
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
        if (\is_object($value)) {
            return $value;
        }

        try {
            $object = (object) $value;

            if (!$object instanceof $this->class) {
                throw new CastingException($value, type_object($this->class));
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
            'type' => 'object',
            'class' => $this->class,
        ];
    }

    public function toString() : string
    {
        return 'object<' . $this->class . '>';
    }
}
