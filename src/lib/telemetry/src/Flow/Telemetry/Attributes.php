<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Immutable container for telemetry attributes.
 *
 * Attributes are key-value pairs attached to spans, logs, metrics, resources,
 * and events. Values can be strings, integers, floats, booleans, DateTimeInterface,
 * Throwable, or arrays of primitive types.
 *
 * Example usage:
 * ```php
 * $attributes = Attributes::create([
 *     'user.id' => '12345',
 *     'user.roles' => ['admin', 'user'],
 *     'timestamp' => new \DateTimeImmutable(),
 *     'error' => $exception,
 * ]);
 * ```
 *
 * @phpstan-type TAttributeValue = array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable
 * @phpstan-type TAttributeValueMap = array<string, TAttributeValue>
 */
final readonly class Attributes
{
    /**
     * @var TAttributeValueMap
     */
    private array $values;

    /**
     * @param array<string, null|TAttributeValue> $values
     */
    public function __construct(array $values = [])
    {
        $this->values = \array_filter($values, static fn ($v) => $v !== null);
    }

    /**
     * Create Attributes from key-value pairs.
     *
     * @param array<string, null|TAttributeValue> $values
     */
    public static function create(array $values = []) : self
    {
        return new self($values);
    }

    /**
     * Create empty Attributes.
     */
    public static function empty() : self
    {
        return new self();
    }

    /**
     * Create Attributes from normalized array.
     *
     * @param array<string, null|TAttributeValue> $data
     */
    public static function fromArray(array $data) : self
    {
        return new self($data);
    }

    /**
     * Get attribute count.
     */
    public function count() : int
    {
        return \count($this->values);
    }

    /**
     * Get a specific attribute value.
     *
     * @return null|TAttributeValue
     */
    public function get(string $key) : string|int|float|bool|\DateTimeInterface|\Throwable|array|null
    {
        return $this->values[$key] ?? null;
    }

    /**
     * Check if attribute exists.
     */
    public function has(string $key) : bool
    {
        return \array_key_exists($key, $this->values);
    }

    /**
     * Create a stable identity string from scalar attributes.
     *
     * Sorts attributes by key and joins them as key=value pairs separated by pipes.
     * Useful for grouping/keying metrics by attribute set.
     *
     * Returns empty string for empty attributes.
     */
    public function id() : string
    {
        if (\count($this->values) === 0) {
            return '';
        }

        $parts = [];

        foreach ($this->values as $key => $value) {
            if ($value === null || \is_array($value)) {
                continue;
            }

            if ($value instanceof \DateTimeInterface) {
                $parts[$key] = $key . '=' . $value->format('c');
            } elseif ($value instanceof \Throwable) {
                $parts[$key] = $key . '=' . $value->getMessage();
            } elseif (\is_bool($value)) {
                $parts[$key] = $key . '=' . ($value ? 'true' : 'false');
            } else {
                $parts[$key] = $key . '=' . (string) $value;
            }
        }

        if (\count($parts) === 0) {
            return '';
        }

        \ksort($parts);

        return \implode('|', $parts);
    }

    /**
     * Check if empty.
     */
    public function isEmpty() : bool
    {
        return \count($this->values) === 0;
    }

    /**
     * Merge with another Attributes instance.
     *
     * The other instance's attributes take precedence over this instance's
     * attributes when keys overlap.
     */
    public function merge(self $other) : self
    {
        return new self(\array_merge($this->values, $other->values));
    }

    /**
     * Normalize to array representation.
     *
     * DateTimeInterface values are converted to ISO 8601 strings.
     * Throwable values are converted to structured arrays with type, message, and stacktrace.
     * Null values are excluded from the result.
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public function normalize() : array
    {
        $result = [];

        foreach ($this->values as $key => $value) {
            if ($value === null) {
                continue;
            }

            $result[$key] = $this->normalizeValue($value);
        }

        return $result;
    }

    /**
     * Create new Attributes with additional attribute.
     *
     * If the key already exists, its value will be replaced.
     *
     * @param TAttributeValue $value
     */
    public function with(string $key, string|int|float|bool|\DateTimeInterface|\Throwable|array $value) : self
    {
        return new self(\array_merge($this->values, [$key => $value]));
    }

    /**
     * Normalize a single value.
     *
     * @param TAttributeValue $value
     *
     * @return array<bool|float|int|string>|bool|float|int|string
     */
    private function normalizeValue(string|int|float|bool|\DateTimeInterface|\Throwable|array $value) : string|int|float|bool|array
    {
        if ($value instanceof \DateTimeInterface) {
            return $value->format('c');
        }

        if ($value instanceof \Throwable) {
            return [
                'type' => $value::class,
                'message' => $value->getMessage(),
                'stacktrace' => $value->getTraceAsString(),
            ];
        }

        if (\is_array($value)) {
            /** @var array<bool|float|int|string> $result */
            $result = \array_map(fn ($v) => $this->normalizeValue($v), $value);

            return $result;
        }

        return $value;
    }
}
