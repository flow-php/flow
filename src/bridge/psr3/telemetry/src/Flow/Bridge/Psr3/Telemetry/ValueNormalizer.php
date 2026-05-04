<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry;

final readonly class ValueNormalizer
{
    /**
     * @return array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable
     */
    public function normalize(mixed $value) : string|int|float|bool|\DateTimeInterface|\Throwable|array
    {
        if ($value === null) {
            return 'null';
        }

        if (\is_scalar($value)) {
            return $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value;
        }

        if ($value instanceof \Throwable) {
            return $value;
        }

        if (\is_array($value)) {
            /** @var array<bool|\DateTimeInterface|float|int|string|\Throwable> $result */
            $result = \array_map(fn ($v) => $this->normalize($v), $value);

            return $result;
        }

        if (\is_object($value)) {
            if (\method_exists($value, '__toString')) {
                return (string) $value;
            }

            return $value::class;
        }

        return \get_debug_type($value);
    }
}
