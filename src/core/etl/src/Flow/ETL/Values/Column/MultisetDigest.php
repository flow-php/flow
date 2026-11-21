<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use function Flow\Types\DSL\type_integer;
use function sprintf;

final readonly class MultisetDigest
{
    public function __construct(
        public int $low,
        public int $high,
    ) {}

    public static function empty(): self
    {
        return new self(0, 0);
    }

    /**
     * @param array<array-key, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(type_integer()->assert($data['low'] ?? null), type_integer()->assert($data['high'] ?? null));
    }

    /**
     * @param int<0, max> $lowLow
     * @param int<0, max> $lowHigh
     * @param int<0, max> $highLow
     * @param int<0, max> $highHigh
     */
    public static function ofHalfSums(int $lowLow, int $lowHigh, int $highLow, int $highHigh): self
    {
        return (new self($lowLow, $highLow))->plus(new self($lowHigh << 32, $highHigh << 32));
    }

    public function plus(self $other): self
    {
        return new self(self::wrappingAdd($this->low, $other->low), self::wrappingAdd($this->high, $other->high));
    }

    /**
     * @return array{low: int, high: int}
     */
    public function normalize(): array
    {
        return ['low' => $this->low, 'high' => $this->high];
    }

    public function hex(): string
    {
        return sprintf('%016x%016x', $this->high, $this->low);
    }

    private static function wrappingAdd(int $a, int $b): int
    {
        $low = ($a & 0xFFFFFFFF) + ($b & 0xFFFFFFFF);
        $high = (($a >> 32) & 0xFFFFFFFF) + (($b >> 32) & 0xFFFFFFFF) + ($low >> 32);

        return (($high & 0xFFFFFFFF) << 32) | ($low & 0xFFFFFFFF);
    }
}
