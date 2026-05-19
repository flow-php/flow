<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

use InvalidArgumentException;
use Stringable;

use function bin2hex;
use function ctype_xdigit;
use function hex2bin;
use function random_bytes;
use function sprintf;
use function str_repeat;
use function strlen;
use function strtolower;

/**
 * A 16-byte (128-bit) trace identifier compatible with OpenTelemetry W3C Trace Context.
 *
 * TraceIds are used to correlate spans across service boundaries.
 * An all-zero TraceId is considered "invalid" and indicates no active trace.
 *
 * Example usage:
 * ```php
 * $traceId = TraceId::generate();
 * echo $traceId->toHex(); // "0af7651916cd43dd8448eb211c80319c"
 *
 * // Invalid trace ID (all zeros)
 * $invalid = TraceId::invalid();
 * echo $invalid->isValid(); // false
 * ```
 */
final readonly class TraceId implements Stringable
{
    public const string INVALID = '00000000000000000000000000000000';

    private const int BYTE_LENGTH = 16;

    private const int HEX_LENGTH = 32;

    private function __construct(
        private string $bytes,
    ) {}

    /**
     * Create a TraceId from a normalized array representation.
     *
     * @param array{hex: string} $data Normalized TraceId data
     *
     * @throws \InvalidArgumentException if the data is invalid
     */
    public static function fromArray(array $data): self
    {
        return self::fromHex($data['hex']);
    }

    /**
     * Create a TraceId from 16 raw bytes.
     *
     * @param string $bytes 16 raw bytes
     *
     * @throws \InvalidArgumentException if the byte string is not exactly 16 bytes
     */
    public static function fromBytes(string $bytes): self
    {
        if (strlen($bytes) !== self::BYTE_LENGTH) {
            throw new InvalidArgumentException(sprintf(
                'TraceId must be exactly %d bytes, got %d',
                self::BYTE_LENGTH,
                strlen($bytes),
            ));
        }

        return new self($bytes);
    }

    /**
     * Create a TraceId from a 32-character hexadecimal string.
     *
     * @param string $hex 32-character lowercase hexadecimal string
     *
     * @throws \InvalidArgumentException if the hex string is invalid
     */
    public static function fromHex(string $hex): self
    {
        if (strlen($hex) !== self::HEX_LENGTH) {
            throw new InvalidArgumentException(sprintf(
                'TraceId hex string must be exactly %d characters, got %d',
                self::HEX_LENGTH,
                strlen($hex),
            ));
        }

        if (!ctype_xdigit($hex)) {
            throw new InvalidArgumentException('TraceId hex string must contain only hexadecimal characters');
        }

        $bytes = hex2bin(strtolower($hex));

        if ($bytes === false) {
            throw new InvalidArgumentException('Failed to decode TraceId hex string');
        }

        return new self($bytes);
    }

    /**
     * Generate a new random TraceId.
     */
    public static function generate(): self
    {
        return new self(random_bytes(self::BYTE_LENGTH));
    }

    /**
     * Get an invalid TraceId (all zeros).
     *
     * Invalid TraceIds indicate no active trace context.
     */
    public static function invalid(): self
    {
        return new self(str_repeat("\0", self::BYTE_LENGTH));
    }

    public function __toString(): string
    {
        return $this->toHex();
    }

    /**
     * Check if this TraceId equals another TraceId.
     */
    public function equals(self $other): bool
    {
        return $this->bytes === $other->bytes;
    }

    /**
     * Check if this TraceId is valid (not all zeros).
     *
     * An all-zero TraceId indicates no active trace context.
     */
    public function isValid(): bool
    {
        return $this->bytes !== str_repeat("\0", self::BYTE_LENGTH);
    }

    /**
     * Normalize the TraceId to an array representation for serialization.
     *
     * @return array{hex: string}
     */
    public function normalize(): array
    {
        return ['hex' => $this->toHex()];
    }

    /**
     * Get the TraceId as 16 raw bytes.
     */
    public function toBytes(): string
    {
        return $this->bytes;
    }

    /**
     * Get the TraceId as a 32-character lowercase hexadecimal string.
     */
    public function toHex(): string
    {
        return bin2hex($this->bytes);
    }
}
