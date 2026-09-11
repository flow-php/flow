<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use DateTimeInterface;
use Throwable;

use function array_filter;
use function array_key_exists;
use function array_map;
use function array_merge;
use function count;
use function get_debug_type;
use function implode;
use function is_array;
use function is_bool;
use function is_object;
use function is_scalar;
use function ksort;
use function method_exists;

/**
 * Immutable container for telemetry attributes.
 *
 * Attributes are key-value pairs attached to spans, logs, metrics, resources,
 * and events. Values can be strings, integers, floats, booleans, DateTimeInterface,
 * Throwable, or arbitrarily nested arrays per the OTel AnyValue specification.
 *
 * Example usage:
 * ```php
 * $attributes = Attributes::create([
 *     'user.id' => '12345',
 *     'user.roles' => ['admin', 'user'],
 *     'user.address' => ['city' => 'Berlin', 'country' => 'DE'],
 *     'timestamp' => new \DateTimeImmutable(),
 *     'error' => $exception,
 * ]);
 * ```
 *
 * @type TAttributeValue = array<array-key, mixed>|bool|\DateTimeInterface|float|int|string|\Throwable
 * @type TAttributeValueMap = array<string, TAttributeValue>
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
        $this->values = array_filter($values, static fn($v) => $v !== null);
    }

    /**
     * @param array<string, null|TAttributeValue> $values
     */
    public static function create(array $values = []): self
    {
        return new self($values);
    }

    public static function empty(): self
    {
        return new self();
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        /** @var array<string, null|TAttributeValue> $data */
        return new self($data);
    }

    /**
     * Get attribute count.
     */
    public function count(): int
    {
        return count($this->values);
    }

    /**
     * The raw attribute map, without normalization.
     *
     * @return TAttributeValueMap
     */
    public function all(): array
    {
        return $this->values;
    }

    /**
     * Get a specific attribute value.
     *
     * @return null|TAttributeValue
     */
    public function get(string $key): string|int|float|bool|DateTimeInterface|Throwable|array|null
    {
        return $this->values[$key] ?? null;
    }

    /**
     * Check if attribute exists.
     */
    public function has(string $key): bool
    {
        return array_key_exists($key, $this->values);
    }

    /**
     * Create a stable identity string from scalar attributes.
     *
     * Sorts attributes by key and joins them as key=value pairs separated by pipes.
     * Useful for grouping/keying metrics by attribute set.
     *
     * Returns empty string for empty attributes.
     */
    public function id(): string
    {
        if (count($this->values) === 0) {
            return '';
        }

        $parts = [];

        foreach ($this->values as $key => $value) {
            if (is_array($value)) {
                continue;
            }

            if ($value instanceof DateTimeInterface) {
                $parts[$key] = $key . '=' . $value->format('c');
            } elseif ($value instanceof Throwable) {
                $parts[$key] = $key . '=' . $value->getMessage();
            } elseif (is_bool($value)) {
                $parts[$key] = $key . '=' . ($value ? 'true' : 'false');
            } else {
                $parts[$key] = $key . '=' . (string) $value;
            }
        }

        if (count($parts) === 0) {
            return '';
        }

        ksort($parts);

        return implode('|', $parts);
    }

    /**
     * Check if empty.
     */
    public function isEmpty(): bool
    {
        return count($this->values) === 0;
    }

    /**
     * Merge with another Attributes instance.
     *
     * The other instance's attributes take precedence over this instance's
     * attributes when keys overlap.
     */
    public function merge(self $other): self
    {
        return new self(array_merge($this->values, $other->values));
    }

    /**
     * Normalize to array representation.
     *
     * DateTimeInterface values are converted to ISO 8601 strings.
     * Throwable values are converted to structured arrays with type, message, and stacktrace.
     * Null values are excluded from the result.
     *
     * @return array<string, array<array-key, mixed>|bool|float|int|string>
     */
    public function normalize(): array
    {
        $result = [];

        foreach ($this->values as $key => $value) {
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
    public function with(string $key, string|int|float|bool|DateTimeInterface|Throwable|array $value): self
    {
        return new self(array_merge($this->values, [$key => $value]));
    }

    /**
     * @return array<array-key, mixed>|bool|float|int|string
     */
    private function normalizeValue(mixed $value): string|int|float|bool|array
    {
        if ($value instanceof DateTimeInterface) {
            return $value->format('c');
        }

        if ($value instanceof Throwable) {
            return [
                'type' => $value::class,
                'message' => $value->getMessage(),
                'stacktrace' => $value->getTraceAsString(),
            ];
        }

        if (is_array($value)) {
            return array_map(fn(mixed $v): string|int|float|bool|array => $this->normalizeValue($v), $value);
        }

        if (is_scalar($value)) {
            return $value;
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        return get_debug_type($value);
    }
}
