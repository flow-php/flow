<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\{Caster, Type};

final class Metadata
{
    public const FROM_NULL = 'from_null';

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $map
     */
    private function __construct(private array $map)
    {
    }

    public static function empty() : self
    {
        return new self([]);
    }

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $map
     */
    public static function fromArray(array $map) : self
    {
        return new self($map);
    }

    /**
     * @param string $key
     * @param array<bool|float|int|string>|bool|float|int|string $value
     */
    public static function with(string $key, int|string|bool|float|array $value) : self
    {
        return new self([$key => $value]);
    }

    /**
     * @param string $key
     * @param array<bool|float|int|string>|bool|float|int|string $value
     */
    public function add(string $key, int|string|bool|float|array $value) : self
    {
        return new self(\array_merge($this->map, [$key => $value]));
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return array<bool|float|int|string>|bool|float|int|string
     */
    public function get(string $key) : int|string|bool|float|array
    {
        if (!\array_key_exists($key, $this->map)) {
            throw new InvalidArgumentException("There no is key: {$key}");
        }

        return $this->map[$key];
    }

    /**
     * @template TType
     *
     * @param Type<TType> $type
     * @param string $key
     * @param TType $default
     *
     * @return ?TType
     */
    public function getAs(string $key, Type $type, mixed $default = null) : mixed
    {
        if (!\array_key_exists($key, $this->map)) {
            return $default;
        }

        return Caster::default()->to($type)->value($this->map[$key]);
    }

    public function has(string $key) : bool
    {
        return \array_key_exists($key, $this->map);
    }

    public function isEqual(self $metadata) : bool
    {
        return (new ArrayComparison())->equals($this->map, $metadata->map);
    }

    public function merge(self $metadata) : self
    {
        return new self(\array_merge($this->map, $metadata->map));
    }

    /**
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public function normalize() : array
    {
        return $this->map;
    }

    public function remove(string $key) : self
    {
        $map = [];

        foreach ($this->map as $currentKey => $value) {
            if ($currentKey !== $key) {
                $map[$currentKey] = $value;
            }
        }

        return new self($map);
    }
}
