<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use DateTimeInterface;
use Flow\Telemetry\Attributes;

use function array_key_exists;
use function array_slice;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function mb_strtolower;
use function preg_match;
use function str_contains;
use function str_ends_with;
use function str_starts_with;

/**
 * Single comparison source for interpreted and compiled paths; MISSING never matches.
 */
final class AttributeMatch
{
    /**
     * Sentinel for an absent path segment; null values are never stored on Attributes.
     */
    public const MISSING = "\0__flow_telemetry_missing__\0";

    /**
     * Resolve a nested attribute path, descending into array values.
     *
     * @param non-empty-list<string> $segments
     *
     * @return mixed the OpenTelemetry AnyValue at the path, or {@see self::MISSING} when any segment is absent
     */
    public static function resolve(Attributes $attributes, array $segments): mixed
    {
        if (!$attributes->has($segments[0])) {
            return self::MISSING;
        }

        $value = $attributes->get($segments[0]);

        foreach (array_slice($segments, 1) as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return self::MISSING;
            }

            // @mago-expect analysis:mixed-assignment
            $value = $value[$segment];
        }

        return $value;
    }

    /**
     * String form of an AnyValue for the substring/regexp modes.
     *
     * Mirrors the scalar handling of {@see Attributes::normalize()}: DateTime is
     * formatted as ISO 8601 and booleans as "true"/"false". Arrays, throwables
     * and the {@see self::MISSING} sentinel have no string form and return null,
     * which the substring/regexp modes treat as "no match".
     */
    public static function stringForm(mixed $value): ?string
    {
        if ($value === self::MISSING) {
            return null;
        }

        if (is_string($value)) {
            return $value;
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if ($value instanceof DateTimeInterface) {
            return $value->format('c');
        }

        return null;
    }

    public static function equal(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        return $value !== self::MISSING && $value === $expected;
    }

    public static function notEqual(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        return $value !== self::MISSING && $value !== $expected;
    }

    public static function greaterThan(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        $order = self::order($value, $expected);

        return $order !== null && $order > 0;
    }

    public static function greaterThanEqual(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        $order = self::order($value, $expected);

        return $order !== null && $order >= 0;
    }

    public static function lessThan(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        $order = self::order($value, $expected);

        return $order !== null && $order < 0;
    }

    public static function lessThanEqual(mixed $value, bool|float|int|string|DateTimeInterface $expected): bool
    {
        $order = self::order($value, $expected);

        return $order !== null && $order <= 0;
    }

    public static function startsWith(?string $value, string $needle, bool $caseSensitive): bool
    {
        if ($value === null) {
            return false;
        }

        if (!$caseSensitive) {
            $value = mb_strtolower($value);
            $needle = mb_strtolower($needle);
        }

        return str_starts_with($value, $needle);
    }

    public static function endsWith(?string $value, string $needle, bool $caseSensitive): bool
    {
        if ($value === null) {
            return false;
        }

        if (!$caseSensitive) {
            $value = mb_strtolower($value);
            $needle = mb_strtolower($needle);
        }

        return str_ends_with($value, $needle);
    }

    public static function contains(?string $value, string $needle, bool $caseSensitive): bool
    {
        if ($value === null) {
            return false;
        }

        if (!$caseSensitive) {
            $value = mb_strtolower($value);
            $needle = mb_strtolower($needle);
        }

        return str_contains($value, $needle);
    }

    public static function regexp(?string $value, string $pattern): bool
    {
        return $value !== null && preg_match($pattern, $value) === 1;
    }

    /**
     * Comparison modes never coerce: a value that is not an orderable scalar or DateTime does not match.
     */
    private static function order(mixed $value, bool|float|int|string|DateTimeInterface $expected): ?int
    {
        if (
            $value !== self::MISSING
            && (
                is_bool($value)
                || is_int($value)
                || is_float($value)
                || is_string($value)
                || $value instanceof DateTimeInterface
            )
        ) {
            return $value <=> $expected;
        }

        return null;
    }
}
