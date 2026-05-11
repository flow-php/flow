<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

/**
 * Key-value store for data propagation across process boundaries.
 *
 * Baggage allows you to propagate arbitrary data along with a trace,
 * similar to HTTP headers but specifically designed for distributed tracing.
 *
 * All operations return new Baggage instances (immutable).
 *
 * Example usage:
 * ```php
 * $baggage = new Baggage(['user.id' => '12345']);
 * $baggage = $baggage->with('request.id', 'abc-123');
 * echo $baggage->get('user.id'); // "12345"
 * ```
 */
final readonly class Baggage implements \Countable
{
    /**
     * @param array<string, string> $entries
     */
    public function __construct(
        private array $entries = [],
    ) {}

    /**
     * Create a Baggage from a normalized array representation.
     *
     * @param array{entries: array<string, string>} $data Normalized Baggage data
     */
    public static function fromArray(array $data): self
    {
        return new self($data['entries']);
    }

    /**
     * Get all entries.
     *
     * @return array<string, string>
     */
    public function all(): array
    {
        return $this->entries;
    }

    /**
     * Get the number of entries.
     */
    public function count(): int
    {
        return \count($this->entries);
    }

    /**
     * Get the value of an entry.
     *
     * @param string $key Entry key
     *
     * @return null|string The entry value, or null if not found
     */
    public function get(string $key): ?string
    {
        return $this->entries[$key] ?? null;
    }

    /**
     * Check if an entry exists.
     *
     * @param string $key Entry key
     */
    public function has(string $key): bool
    {
        return \array_key_exists($key, $this->entries);
    }

    /**
     * Check if the baggage is empty.
     */
    public function isEmpty(): bool
    {
        return \count($this->entries) === 0;
    }

    /**
     * Normalize the Baggage to an array representation for serialization.
     *
     * @return array{entries: array<string, string>}
     */
    public function normalize(): array
    {
        return ['entries' => $this->entries];
    }

    /**
     * Create a new Baggage with an additional entry.
     *
     * If the key already exists, its value will be replaced.
     *
     * @param string $key Entry key
     * @param string $value Entry value
     *
     * @return self New Baggage instance with the added entry
     */
    public function with(string $key, string $value): self
    {
        return new self(\array_merge($this->entries, [$key => $value]));
    }

    /**
     * Create a new Baggage without the specified entry.
     *
     * @param string $key Entry key to remove
     *
     * @return self New Baggage instance without the entry
     */
    public function without(string $key): self
    {
        $entries = $this->entries;
        unset($entries[$key]);

        return new self($entries);
    }
}
