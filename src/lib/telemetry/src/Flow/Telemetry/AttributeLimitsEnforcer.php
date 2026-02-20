<?php

declare(strict_types=1);

namespace Flow\Telemetry;

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
    public function enforce(
        Attributes $attributes,
        int $countLimit,
        ?int $valueLengthLimit,
    ) : EnforcedAttributes {
        $normalized = $attributes->normalize();
        $droppedCount = 0;

        if (\count($normalized) > $countLimit) {
            $droppedCount = \count($normalized) - $countLimit;
            $normalized = \array_slice($normalized, 0, $countLimit, true);
        }

        if ($valueLengthLimit !== null) {
            $normalized = $this->truncateStringValues($normalized, $valueLengthLimit);
        }

        return new EnforcedAttributes(
            Attributes::fromArray($normalized),
            $droppedCount,
        );
    }

    /**
     * @param array<bool|float|int|string> $values
     *
     * @return array<bool|float|int|string>
     */
    private function truncateArrayValues(array $values, int $maxLength) : array
    {
        foreach ($values as $index => $value) {
            if (\is_string($value) && \mb_strlen($value) > $maxLength) {
                $values[$index] = \mb_substr($value, 0, $maxLength);
            }
        }

        return $values;
    }

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $values
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    private function truncateStringValues(array $values, int $maxLength) : array
    {
        foreach ($values as $key => $value) {
            if (\is_string($value) && \mb_strlen($value) > $maxLength) {
                $values[$key] = \mb_substr($value, 0, $maxLength);
            } elseif (\is_array($value)) {
                $values[$key] = $this->truncateArrayValues($value, $maxLength);
            }
        }

        return $values;
    }
}
