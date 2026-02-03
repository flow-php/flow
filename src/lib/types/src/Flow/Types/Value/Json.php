<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;

final readonly class Json implements \JsonSerializable, \Stringable
{
    private bool $isObject;

    private string $value;

    public function __construct(string $value)
    {
        if (!self::isValid($value)) {
            throw new InvalidArgumentException("Invalid JSON: '{$value}'");
        }

        $this->value = $value;
        $this->isObject = \str_starts_with($value, '{') && \str_ends_with($value, '}');
    }

    /**
     * @param array<array-key, mixed> $value
     */
    public static function fromArray(array $value, bool $asObject = false) : self
    {
        if ($asObject && [] === $value) {
            return new self('{}');
        }

        return new self(\json_encode($value, \JSON_THROW_ON_ERROR));
    }

    public static function fromString(string $value) : self
    {
        return new self($value);
    }

    public static function isValid(string $value) : bool
    {
        if ($value === '') {
            return false;
        }

        if ('{' !== $value[0] && '[' !== $value[0]) {
            return false;
        }

        if (
            !(\str_starts_with($value, '{') && \str_ends_with($value, '}'))
            && !(\str_starts_with($value, '[') && \str_ends_with($value, ']'))
        ) {
            return false;
        }

        return \json_validate($value);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function isEqual(self $json) : bool
    {
        $a = $this->sortRecursive($this->toArray());
        $b = $this->sortRecursive($json->toArray());

        return \json_encode($a, \JSON_THROW_ON_ERROR) === \json_encode($b, \JSON_THROW_ON_ERROR);
    }

    public function isObject() : bool
    {
        return $this->isObject;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function jsonSerialize() : array
    {
        return $this->toArray();
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray() : array
    {
        return (array) \json_decode($this->value, true, flags: \JSON_THROW_ON_ERROR);
    }

    public function toString() : string
    {
        return $this->value;
    }

    /**
     * @param array<array-key, mixed> $array
     *
     * @return array<array-key, mixed>
     */
    private function sortRecursive(array $array) : array
    {
        foreach ($array as $key => $value) {
            if (\is_array($value)) {
                $array[$key] = $this->sortRecursive($value);
            }
        }

        if (\array_is_list($array)) {
            \usort($array, static fn (mixed $a, mixed $b) : int => \serialize($a) <=> \serialize($b));
        } else {
            \ksort($array);
        }

        return $array;
    }
}
