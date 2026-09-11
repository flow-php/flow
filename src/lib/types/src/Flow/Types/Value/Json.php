<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;
use JsonSerializable;
use Stringable;

use function array_is_list;
use function array_keys;
use function Flow\Types\DSL\type_array;
use function is_array;
use function json_decode;
use function json_encode;
use function json_validate;
use function ksort;
use function serialize;
use function str_ends_with;
use function str_starts_with;
use function usort;

use const JSON_THROW_ON_ERROR;

final readonly class Json implements JsonSerializable, Stringable
{
    private bool $isObject;

    private string $value;

    public function __construct(string $value)
    {
        if (!self::isValid($value)) {
            throw new InvalidArgumentException("Invalid JSON: '{$value}'");
        }

        $this->value = $value;
        $this->isObject = str_starts_with($value, '{') && str_ends_with($value, '}');
    }

    /**
     * @param array<array-key, mixed> $value
     */
    public static function fromArray(array $value, bool $asObject = false): self
    {
        if ($asObject) {
            if ([] === $value) {
                return new self('{}');
            }

            foreach (array_keys($value) as $key) {
                if (!is_string($key)) {
                    throw new InvalidArgumentException('All keys of a JSON object must be strings');
                }
            }
        }

        return new self(json_encode($value, JSON_THROW_ON_ERROR));
    }

    public static function fromString(string $value): self
    {
        return new self($value);
    }

    public static function isValid(string $value): bool
    {
        if (
            !(str_starts_with($value, '{') && str_ends_with($value, '}'))
            && !(str_starts_with($value, '[') && str_ends_with($value, ']'))
        ) {
            return false;
        }

        return json_validate($value);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function isEqual(self $json): bool
    {
        $a = $this->sortRecursive($this->toArray());
        $b = $this->sortRecursive($json->toArray());

        return json_encode($a, JSON_THROW_ON_ERROR) === json_encode($b, JSON_THROW_ON_ERROR);
    }

    public function isObject(): bool
    {
        return $this->isObject;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(): array
    {
        return type_array()->assert(json_decode($this->value, true, flags: JSON_THROW_ON_ERROR));
    }

    public function toString(): string
    {
        return $this->value;
    }

    /**
     * @param array<array-key, mixed> $array
     *
     * @return array<array-key, mixed>
     */
    private function sortRecursive(array $array): array
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $array[$key] = $this->sortRecursive($value);
            }
        }

        if (array_is_list($array)) {
            usort($array, static fn(mixed $a, mixed $b): int => serialize($a) <=> serialize($b));
        } else {
            ksort($array);
        }

        return $array;
    }
}
