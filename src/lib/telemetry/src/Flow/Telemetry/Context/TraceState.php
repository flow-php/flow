<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

/**
 * W3C Trace Context tracestate header value.
 *
 * TraceState provides vendor-specific trace identification data and enables
 * multiple tracing systems to participate in the same trace. It's an immutable
 * list of key-value pairs with ordering preserved.
 *
 * Example usage:
 * ```php
 * $state = TraceState::empty();
 * $state = $state->with('vendor', 'value');
 * echo $state->toString(); // "vendor=value"
 * ```
 *
 * @see https://www.w3.org/TR/trace-context/#tracestate-header
 */
final readonly class TraceState implements \Stringable
{
    private const string KEY_PATTERN = '/^(?:[a-z][a-z0-9_\-*\/]{0,255}|[a-z0-9][a-z0-9_\-*\/]{0,240}@[a-z][a-z0-9_\-*\/]{0,13})$/';

    private const int MAX_ENTRIES = 32;

    /**
     * @param array<string, string> $entries Key-value pairs in insertion order
     */
    private function __construct(
        private array $entries,
    ) {
    }

    /**
     * Create an empty TraceState.
     */
    public static function empty() : self
    {
        return new self([]);
    }

    /**
     * Create TraceState from a normalized array representation.
     *
     * @param array{entries: array<string, string>} $data Normalized TraceState data
     */
    public static function fromArray(array $data) : self
    {
        return new self($data['entries']);
    }

    /**
     * Create TraceState from W3C tracestate header string.
     *
     * @param string $string W3C tracestate header value
     *
     * @throws \InvalidArgumentException if the string is malformed
     */
    public static function fromString(string $string) : self
    {
        if ($string === '') {
            return self::empty();
        }

        $entries = [];
        $pairs = \explode(',', $string);

        foreach ($pairs as $pair) {
            $pair = \trim($pair);

            if ($pair === '') {
                continue;
            }

            $parts = \explode('=', $pair, 2);

            if (\count($parts) !== 2) {
                throw new \InvalidArgumentException(\sprintf(
                    'Invalid tracestate entry: "%s"',
                    $pair
                ));
            }

            [$key, $value] = $parts;
            $key = \trim($key);
            $value = \trim($value);

            self::validateKey($key);
            self::validateValue($value);

            if (\count($entries) >= self::MAX_ENTRIES) {
                break;
            }

            $entries[$key] = $value;
        }

        return new self($entries);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * Get all entries as an array.
     *
     * @return array<string, string>
     */
    public function all() : array
    {
        return $this->entries;
    }

    /**
     * Check if this TraceState equals another TraceState.
     */
    public function equals(self $other) : bool
    {
        return $this->entries === $other->entries;
    }

    /**
     * Get a value by key.
     *
     * @return null|string The value or null if not found
     */
    public function get(string $key) : ?string
    {
        return $this->entries[$key] ?? null;
    }

    /**
     * Check if this TraceState has any entries.
     */
    public function isEmpty() : bool
    {
        return $this->entries === [];
    }

    /**
     * Normalize the TraceState to an array representation for serialization.
     *
     * @return array{entries: array<string, string>}
     */
    public function normalize() : array
    {
        return ['entries' => $this->entries];
    }

    /**
     * Convert to W3C tracestate header string.
     */
    public function toString() : string
    {
        if ($this->entries === []) {
            return '';
        }

        $pairs = [];

        foreach ($this->entries as $key => $value) {
            $pairs[] = $key . '=' . $value;
        }

        return \implode(',', $pairs);
    }

    /**
     * Create a new TraceState with a key-value pair added or updated.
     *
     * New entries are added at the beginning (most recent position).
     *
     * @throws \InvalidArgumentException if the key or value is invalid
     */
    public function with(string $key, string $value) : self
    {
        self::validateKey($key);
        self::validateValue($value);

        $entries = $this->entries;
        unset($entries[$key]);

        $entries = [$key => $value] + $entries;

        if (\count($entries) > self::MAX_ENTRIES) {
            $entries = \array_slice($entries, 0, self::MAX_ENTRIES, true);
        }

        return new self($entries);
    }

    /**
     * Create a new TraceState with a key removed.
     */
    public function without(string $key) : self
    {
        if (!isset($this->entries[$key])) {
            return $this;
        }

        $entries = $this->entries;
        unset($entries[$key]);

        return new self($entries);
    }

    /**
     * Validate a tracestate key per W3C spec.
     *
     * @throws \InvalidArgumentException if the key is invalid
     */
    private static function validateKey(string $key) : void
    {
        if ($key === '') {
            throw new \InvalidArgumentException('TraceState key cannot be empty');
        }

        if (!\preg_match(self::KEY_PATTERN, $key)) {
            throw new \InvalidArgumentException(\sprintf(
                'Invalid TraceState key: "%s"',
                $key
            ));
        }
    }

    /**
     * Validate a tracestate value per W3C spec.
     *
     * @throws \InvalidArgumentException if the value is invalid
     */
    private static function validateValue(string $value) : void
    {
        if ($value === '') {
            throw new \InvalidArgumentException('TraceState value cannot be empty');
        }

        if (\strlen($value) > 256) {
            throw new \InvalidArgumentException(\sprintf(
                'TraceState value exceeds maximum length of 256: %d',
                \strlen($value)
            ));
        }

        for ($i = 0; $i < \strlen($value); $i++) {
            $ord = \ord($value[$i]);

            if ($ord < 0x20 || $ord > 0x7E || $value[$i] === ',' || $value[$i] === '=') {
                throw new \InvalidArgumentException(\sprintf(
                    'TraceState value contains invalid character at position %d',
                    $i
                ));
            }
        }
    }
}
