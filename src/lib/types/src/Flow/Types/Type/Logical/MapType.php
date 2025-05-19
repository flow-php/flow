<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type\Native\{IntegerType, StringType};
use Flow\Types\Type\{Type, TypeFactory};

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Type<array<TKey, TValue>>
 */
final readonly class MapType implements Type
{
    /**
     * @param Type<TValue> $value
     */
    public function __construct(private StringType|IntegerType $key, private Type $value)
    {
    }

    /**
     * @param array{type: 'map', key: array, value: array} $data
     *
     * @return MapType<array-key, Type<mixed>>
     */
    public static function fromArray(array $data) : self
    {
        $keyType = TypeFactory::fromArray($data['key']);

        if (!$keyType instanceof StringType && !$keyType instanceof IntegerType) {
            throw new InvalidArgumentException('Map key must be string or integer');
        }

        return new self($keyType, TypeFactory::fromArray($data['value']));
    }

    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : array
    {
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            $castedMap = [];

            foreach ($value as $key => $item) {
                $castedKey = $this->key->cast($key);

                if (\array_key_exists($castedKey, $castedMap)) {
                    throw new CastingException($value, $this);
                }

                $castedMap[$this->key->cast($key)] = $this->value->cast($item);
            }

            return $this->assert($castedMap);
        } catch (\Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        foreach ($value as $key => $item) {
            if (!$this->key->isValid($key)) {
                return false;
            }

            if (!$this->value->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    public function key() : StringType|IntegerType
    {
        return $this->key;
    }

    /**
     * @return array{type: 'map', key: array, value: array}
     */
    public function normalize() : array
    {
        return [
            'type' => 'map',
            'key' => $this->key->normalize(),
            'value' => $this->value->normalize(),
        ];
    }

    public function toString() : string
    {
        return 'map<' . $this->key->toString() . ', ' . $this->value->toString() . '>';
    }

    /**
     * @return Type<mixed>
     */
    public function value() : Type
    {
        return $this->value;
    }
}
