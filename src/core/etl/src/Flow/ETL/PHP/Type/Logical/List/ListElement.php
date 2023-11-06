<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\List;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class ListElement
{
    private function __construct(private readonly ScalarType|ObjectType $value)
    {
    }

    public static function boolean() : self
    {
        return new self(ScalarType::boolean);
    }

    public static function float() : self
    {
        return new self(ScalarType::float);
    }

    public static function fromString(string $value) : self
    {
        try {
            return new self(ScalarType::fromString($value));
        } catch (InvalidArgumentException) {
            /**
             * @psalm-suppress ArgumentTypeCoercion
             *
             * @phpstan-ignore-next-line
             */
            return new self(ObjectType::fromString($value));
        }
    }

    public static function integer() : self
    {
        return new self(ScalarType::integer);
    }

    /**
     * @param class-string $class
     */
    public static function object(string $class) : self
    {
        return new self(new ObjectType($class));
    }

    public static function string() : self
    {
        return new self(ScalarType::string);
    }

    public function isEqual(mixed $value) : bool
    {
        return $this->value->isEqual($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->value->isValid($value);
    }

    public function toString() : string
    {
        return $this->value->toString();
    }

    public function value() : ScalarType|ObjectType
    {
        return $this->value;
    }
}
