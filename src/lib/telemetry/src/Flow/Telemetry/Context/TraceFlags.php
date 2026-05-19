<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

use InvalidArgumentException;
use Stringable;

use function ctype_xdigit;
use function dechex;
use function hexdec;
use function sprintf;
use function str_pad;
use function strlen;

/**
 * W3C Trace Context trace flags (single byte).
 *
 * TraceFlags contain information about the trace. Currently defined flags:
 * - SAMPLED (bit 0): Whether the trace should be sampled/exported
 * - RANDOM (bit 1): Whether the trace-id was randomly generated
 *
 * Example usage:
 * ```php
 * $flags = TraceFlags::default();
 * $flags = $flags->withSampled(true);
 * echo $flags->toHex(); // "01"
 * ```
 *
 * @see https://www.w3.org/TR/trace-context/#trace-flags
 */
final readonly class TraceFlags implements Stringable
{
    public const int RANDOM = 0x02;

    public const int SAMPLED = 0x01;

    private function __construct(
        private int $flags,
    ) {}

    /**
     * Create TraceFlags with default values (no flags set).
     */
    public static function default(): self
    {
        return new self(0);
    }

    /**
     * Create TraceFlags from a normalized array representation.
     *
     * @param array{byte: int} $data Normalized TraceFlags data
     */
    public static function fromArray(array $data): self
    {
        return self::fromByte($data['byte']);
    }

    /**
     * Create TraceFlags from a byte value.
     *
     * @param int $byte The flags byte (0-255)
     *
     * @throws \InvalidArgumentException if the byte is out of range
     */
    public static function fromByte(int $byte): self
    {
        if ($byte < 0 || $byte > 255) {
            throw new InvalidArgumentException(sprintf('TraceFlags byte must be between 0 and 255, got %d', $byte));
        }

        return new self($byte);
    }

    /**
     * Create TraceFlags from a 2-character hexadecimal string.
     *
     * @param string $hex 2-character lowercase hexadecimal string
     *
     * @throws \InvalidArgumentException if the hex string is invalid
     */
    public static function fromHex(string $hex): self
    {
        if (strlen($hex) !== 2) {
            throw new InvalidArgumentException(sprintf(
                'TraceFlags hex string must be exactly 2 characters, got %d',
                strlen($hex),
            ));
        }

        if (!ctype_xdigit($hex)) {
            throw new InvalidArgumentException('TraceFlags hex string must contain only hexadecimal characters');
        }

        return new self((int) hexdec($hex));
    }

    /**
     * Create TraceFlags with SAMPLED flag set.
     */
    public static function sampled(): self
    {
        return new self(self::SAMPLED);
    }

    public function __toString(): string
    {
        return $this->toHex();
    }

    /**
     * Check if this TraceFlags equals another TraceFlags.
     */
    public function equals(self $other): bool
    {
        return $this->flags === $other->flags;
    }

    /**
     * Check if the RANDOM flag is set.
     */
    public function isRandom(): bool
    {
        return ($this->flags & self::RANDOM) === self::RANDOM;
    }

    /**
     * Check if the SAMPLED flag is set.
     */
    public function isSampled(): bool
    {
        return ($this->flags & self::SAMPLED) === self::SAMPLED;
    }

    /**
     * Normalize the TraceFlags to an array representation for serialization.
     *
     * @return array{byte: int}
     */
    public function normalize(): array
    {
        return ['byte' => $this->flags];
    }

    /**
     * Get the flags as a byte value.
     */
    public function toByte(): int
    {
        return $this->flags;
    }

    /**
     * Get the flags as a 2-character lowercase hexadecimal string.
     */
    public function toHex(): string
    {
        return str_pad(dechex($this->flags), 2, '0', STR_PAD_LEFT);
    }

    /**
     * Create a new TraceFlags with the RANDOM flag set or unset.
     */
    public function withRandom(bool $random = true): self
    {
        if ($random) {
            return new self($this->flags | self::RANDOM);
        }

        return new self($this->flags & ~self::RANDOM);
    }

    /**
     * Create a new TraceFlags with the SAMPLED flag set or unset.
     */
    public function withSampled(bool $sampled = true): self
    {
        if ($sampled) {
            return new self($this->flags | self::SAMPLED);
        }

        return new self($this->flags & ~self::SAMPLED);
    }
}
