<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;

/**
 * @psalm-allow-private-mutation
 */
final class Metadata
{
    /**
     * @var array<string, array<mixed>|bool|float|int|object|string>
     */
    private array $map;

    /**
     * @param array<string, array<mixed>|bool|float|int|object|string> $map
     */
    private function __construct(array $map)
    {
        $this->map = $map;
    }

    public static function empty() : self
    {
        return new self([]);
    }

    /**
     * @param string $key
     * @param array<mixed>|bool|float|int|object|string $value
     *
     * @return $this
     */
    public static function with(string $key, int|string|bool|float|object|array $value) : self
    {
        return new self([$key => $value]);
    }

    public function __serialize() : array
    {
        return [
            'map' => $this->map,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->map = $data['map'];
    }

    /**
     * @param string $key
     * @param array<mixed>|bool|float|int|object|string $value
     *
     * @return $this
     */
    public function add(string $key, int|string|bool|float|object|array $value) : self
    {
        return new self(\array_merge($this->map, [$key => $value]));
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return array<mixed>|bool|float|int|object|string
     */
    public function get(string $key) : int|string|bool|float|object|array
    {
        if (!\array_key_exists($key, $this->map)) {
            throw new InvalidArgumentException("There no is key: {$key}");
        }

        return $this->map[$key];
    }

    public function isEqual(self $metadata) : bool
    {
        return (new ArrayComparison())->equals($this->map, $metadata->map);
    }

    public function merge(self $metadata) : self
    {
        return new self(\array_merge($this->map, $metadata->map));
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
