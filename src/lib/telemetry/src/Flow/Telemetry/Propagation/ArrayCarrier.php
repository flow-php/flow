<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use function strtolower;

/**
 * Key lookups are case-insensitive (HTTP headers).
 *
 * @implements Carrier<array<string, string>>
 */
final class ArrayCarrier implements Carrier
{
    /**
     * @param array<string, string> $data The carrier data
     */
    public function __construct(
        private array $data = [],
    ) {}

    public function get(string $key): ?string
    {
        $lowerKey = strtolower($key);

        foreach ($this->data as $k => $v) {
            if (strtolower($k) === $lowerKey) {
                return $v;
            }
        }

        return null;
    }

    public function set(string $key, string $value): static
    {
        $this->data[$key] = $value;

        return $this;
    }

    /**
     * @return array<string, string>
     */
    public function unwrap(): array
    {
        return $this->data;
    }
}
