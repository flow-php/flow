<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function hex2bin;
use function preg_match;
use function sprintf;
use function unpack;

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

    public static function fromHex(string $hex): self
    {
        if (preg_match('/^[0-9a-f]{32}$/', $hex) !== 1) {
            throw new InvalidArgumentException(sprintf(
                'A multiset digest is 32 lowercase hex characters, high lane first, given: "%s"',
                $hex,
            ));
        }

        /** @var array{1: int, 2: int} $lanes */
        $lanes = unpack('J2', (string) hex2bin($hex));

        return new self($lanes[2], $lanes[1]);
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
