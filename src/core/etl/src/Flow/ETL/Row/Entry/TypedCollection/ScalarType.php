<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry\TypedCollection;

use Flow\ETL\Exception\InvalidArgumentException;

enum ScalarType : string implements Type
{
    case boolean = 'boolean';
    case float = 'float';
    case integer = 'integer';
    case string = 'string';

    public static function fromString(string $value) : self
    {
        return match (\strtolower($value)) {
            'integer' => self::integer,
            'float', 'double' => self::float,
            'string' => self::string,
            'boolean' => self::boolean,
            default => throw new InvalidArgumentException("Unsupported scalar type: {$value}")
        };
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $type->value === $this->value;
    }

    public function isValid(array $collection) : bool
    {
        if (!\array_is_list($collection)) {
            return false;
        }

        foreach ($collection as $value) {
            if (!\is_scalar($value)) {
                return false;
            }

            if ($this->value === 'float') {
                // php gettype returns double for floats for historical reasons
                if ('double' !== \gettype($value)) {
                    return false;
                }
            } else {
                if ($this->value !== \gettype($value)) {
                    return false;
                }
            }
        }

        return true;
    }

    public function toString() : string
    {
        return $this->value;
    }
}
