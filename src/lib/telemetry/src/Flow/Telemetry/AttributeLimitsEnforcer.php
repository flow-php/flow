<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use function array_map;
use function array_slice;
use function count;
use function is_array;
use function is_string;
use function mb_strlen;
use function mb_substr;

/**
 * Stateless service that enforces attribute limits per OpenTelemetry specification.
 *
 * Applies two types of limits:
 * - Count limit: keeps first N attributes, drops the rest
 * - Value length limit: truncates string values using UTF-8 safe substring
 *
 * @see https://opentelemetry.io/docs/specs/otel/common/
 */
final readonly class AttributeLimitsEnforcer
{
    /**
     * Enforce limits on attributes.
     *
     * @param Attributes $attributes The attributes to enforce limits on
     * @param int $countLimit Maximum number of attributes to keep
     * @param null|int $valueLengthLimit Maximum length for string values (null = unlimited)
     */
    public function enforce(Attributes $attributes, int $countLimit, ?int $valueLengthLimit): EnforcedAttributes
    {
        $normalized = $attributes->normalize();
        $droppedCount = 0;

        if (count($normalized) > $countLimit) {
            $droppedCount = count($normalized) - $countLimit;
            $normalized = array_slice($normalized, 0, $countLimit, true);
        }

        if ($valueLengthLimit !== null) {
            $normalized = $this->truncateStringValues($normalized, $valueLengthLimit);
        }

        return new EnforcedAttributes(Attributes::fromArray($normalized), $droppedCount);
    }

    /**
     * @param array<array-key, mixed> $values
     *
     * @return array<array-key, mixed>
     */
    private function truncateArrayValues(array $values, int $maxLength): array
    {
        return array_map(function (mixed $value) use ($maxLength): mixed {
            if (is_string($value) && mb_strlen($value) > $maxLength) {
                return mb_substr($value, 0, $maxLength);
            }

            if (is_array($value)) {
                return $this->truncateArrayValues($value, $maxLength);
            }

            return $value;
        }, $values);
    }

    /**
     * @param array<string, array<array-key, mixed>|bool|float|int|string> $values
     *
     * @return array<string, array<array-key, mixed>|bool|float|int|string>
     */
    private function truncateStringValues(array $values, int $maxLength): array
    {
        foreach ($values as $key => $value) {
            if (is_string($value) && mb_strlen($value) > $maxLength) {
                $values[$key] = mb_substr($value, 0, $maxLength);
            } elseif (is_array($value)) {
                $values[$key] = $this->truncateArrayValues($value, $maxLength);
            }
        }

        return $values;
    }
}
