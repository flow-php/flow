<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

/**
 * An 8-byte (64-bit) span identifier compatible with OpenTelemetry W3C Trace Context.
 *
 * SpanIds uniquely identify a span within a trace.
 * All-zero SpanIds are rejected at construction time per W3C spec.
 *
 * Example usage:
 * ```php
 * $spanId = SpanId::generate();
 * echo $spanId->toHex(); // "00f067aa0ba902b7"
 * ```
 */
final readonly class SpanId implements \Stringable
{
    private const int BYTE_LENGTH = 8;

    private const int HEX_LENGTH = 16;

    private function __construct(
        private string $bytes,
    ) {
    }

    /**
     * Create a SpanId from a normalized array representation.
     *
     * @param array{hex: string} $data Normalized SpanId data
     *
     * @throws \InvalidArgumentException if the data is invalid
     */
    public static function fromArray(array $data) : self
    {
        return self::fromHex($data['hex']);
    }

    /**
     * Create a SpanId from 8 raw bytes.
     *
     * @param string $bytes 8 raw bytes
     *
     * @throws \InvalidArgumentException if the byte string is not exactly 8 bytes or is all zeros
     */
    public static function fromBytes(string $bytes) : self
    {
        if (\strlen($bytes) !== self::BYTE_LENGTH) {
            throw new \InvalidArgumentException(\sprintf(
                'SpanId must be exactly %d bytes, got %d',
                self::BYTE_LENGTH,
                \strlen($bytes)
            ));
        }

        if ($bytes === \str_repeat("\0", self::BYTE_LENGTH)) {
            throw new \InvalidArgumentException('SpanId cannot be all zeros');
        }

        return new self($bytes);
    }

    /**
     * Create a SpanId from a 16-character hexadecimal string.
     *
     * @param string $hex 16-character lowercase hexadecimal string
     *
     * @throws \InvalidArgumentException if the hex string is invalid or represents all zeros
     */
    public static function fromHex(string $hex) : self
    {
        if (\strlen($hex) !== self::HEX_LENGTH) {
            throw new \InvalidArgumentException(\sprintf(
                'SpanId hex string must be exactly %d characters, got %d',
                self::HEX_LENGTH,
                \strlen($hex)
            ));
        }

        if (!\ctype_xdigit($hex)) {
            throw new \InvalidArgumentException('SpanId hex string must contain only hexadecimal characters');
        }

        $bytes = \hex2bin(\strtolower($hex));

        if ($bytes === false) {
            throw new \InvalidArgumentException('Failed to decode SpanId hex string');
        }

        if ($bytes === \str_repeat("\0", self::BYTE_LENGTH)) {
            throw new \InvalidArgumentException('SpanId cannot be all zeros');
        }

        return new self($bytes);
    }

    /**
     * Generate a new random SpanId.
     */
    public static function generate() : self
    {
        return new self(\random_bytes(self::BYTE_LENGTH));
    }

    public function __toString() : string
    {
        return $this->toHex();
    }

    /**
     * Check if this SpanId equals another SpanId.
     */
    public function equals(self $other) : bool
    {
        return $this->bytes === $other->bytes;
    }

    /**
     * Normalize the SpanId to an array representation for serialization.
     *
     * @return array{hex: string}
     */
    public function normalize() : array
    {
        return ['hex' => $this->toHex()];
    }

    /**
     * Get the SpanId as 8 raw bytes.
     */
    public function toBytes() : string
    {
        return $this->bytes;
    }

    /**
     * Get the SpanId as a 16-character lowercase hexadecimal string.
     */
    public function toHex() : string
    {
        return \bin2hex($this->bytes);
    }
}
