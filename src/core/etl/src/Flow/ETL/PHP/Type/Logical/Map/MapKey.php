<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\LogicalType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\TypeFactory;

final class MapKey
{
    public function __construct(private readonly ScalarType|LogicalType $value)
    {
    }

    public static function datetime() : self
    {
        return new self(type_datetime(false));
    }

    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('type', $data)) {
            throw new InvalidArgumentException('Missing "type" key in ' . self::class . ' fromArray()');
        }

        $keyType = TypeFactory::fromArray($data['type']);

        if (!$keyType instanceof ScalarType && !$keyType instanceof LogicalType) {
            throw new InvalidArgumentException('Invalid "type" key in ' . self::class . ' fromArray()');
        }

        return new self($keyType);
    }

    public static function fromType(ScalarType|LogicalType $type) : self
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

    public static function uuid() : self
    {
        return new self(type_uuid(false));
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

    public function type() : ScalarType|LogicalType
    {
        return $this->value;
    }
}
