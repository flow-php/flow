<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use function Flow\ETL\DSL\{type_int, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Native\{IntegerType, StringType};
use Flow\ETL\PHP\Type\TypeFactory;

final class MapKey
{
    public function __construct(private readonly IntegerType|StringType $value)
    {
    }

    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('type', $data)) {
            throw new InvalidArgumentException('Missing "type" key in ' . self::class . ' fromArray()');
        }

        $keyType = TypeFactory::fromArray($data['type']);

        if (!$keyType instanceof IntegerType && !$keyType instanceof StringType && !$keyType instanceof UuidType) {
            throw new InvalidArgumentException('Invalid "type" key in ' . self::class . ' fromArray()');
        }

        return new self($keyType);
    }

    public static function fromType(IntegerType|StringType $type) : self
    {
        return new self($type);
    }

    public static function integer() : self
    {
        return new self(type_int(false));
    }

    public static function string() : self
    {
        return new self(type_string(false));
    }

    public function isEqual(mixed $value) : bool
    {
        return $this->value->isEqual($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->value->isValid($value);
    }

    public function normalize() : array
    {
        return [
            'type' => $this->value->normalize(),
        ];
    }

    public function toString() : string
    {
        return $this->value->toString();
    }

    public function type() : StringType|IntegerType
    {
        return $this->value;
    }
}
