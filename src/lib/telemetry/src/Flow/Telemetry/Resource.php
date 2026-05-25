<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use DateTimeInterface;
use Throwable;

/**
 * Entity metadata attached to all telemetry signals.
 *
 * A Resource represents the entity producing telemetry data, such as a
 * service, container, or host. Resources are immutable and provide
 * context that is attached to all traces, logs, and metrics.
 *
 * Common resource attributes include:
 * - service.name: The logical name of the service
 * - service.version: The version of the service
 * - host.name: The hostname of the machine
 * - process.pid: The process ID
 *
 * Example usage:
 * ```php
 * $resource = Resource::create([
 *     'service.name' => 'my-service',
 *     'service.version' => '1.0.0',
 * ]);
 * ```
 *
 * @phpstan-import-type TAttributeValue from Attributes
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final readonly class Resource
{
    public function __construct(
        public Attributes $attributes = new Attributes(),
    ) {}

    /**
     * @param TAttributeValueMap|Attributes $attributes
     */
    public static function create(Attributes|array $attributes = []): self
    {
        return new self($attributes instanceof Attributes ? $attributes : Attributes::create($attributes));
    }

    /**
     * Create an empty Resource with no attributes.
     */
    public static function empty(): self
    {
        return new self();
    }

    /**
     * Create a Resource from a normalized array representation.
     *
     * @param array{attributes: array<string, mixed>} $data Normalized Resource data
     */
    public static function fromArray(array $data): self
    {
        return new self(Attributes::fromArray($data['attributes']));
    }

    /**
     * Get all attributes.
     *
     * @return array<string, array<array-key, mixed>|bool|float|int|string>
     */
    public function all(): array
    {
        return $this->attributes->normalize();
    }

    /**
     * Get the number of attributes.
     */
    public function count(): int
    {
        return $this->attributes->count();
    }

    /**
     * Get the value of an attribute.
     *
     * @param string $key Attribute key
     *
     * @return null|TAttributeValue The attribute value, or null if not found
     */
    public function get(string $key): string|int|float|bool|DateTimeInterface|Throwable|array|null
    {
        return $this->attributes->get($key);
    }

    /**
     * Check if an attribute exists.
     *
     * @param string $key Attribute key
     */
    public function has(string $key): bool
    {
        return $this->attributes->has($key);
    }

    /**
     * Check if the resource has no attributes.
     */
    public function isEmpty(): bool
    {
        return $this->attributes->isEmpty();
    }

    /**
     * Merge this Resource with another, with the other's attributes taking precedence.
     *
     * @param self $other The Resource to merge with
     *
     * @return self New Resource with merged attributes
     */
    public function merge(self $other): self
    {
        return new self($this->attributes->merge($other->attributes));
    }

    /**
     * Normalize the Resource to an array representation for serialization.
     *
     * @return array{attributes: array<string, mixed>}
     */
    public function normalize(): array
    {
        return [
            'attributes' => $this->attributes->normalize(),
        ];
    }

    /**
     * Create a new Resource with an additional attribute.
     *
     * If the key already exists, its value will be replaced.
     *
     * @param string $key Attribute key
     * @param TAttributeValue $value Attribute value
     *
     * @return self New Resource with the added attribute
     */
    public function with(string $key, string|int|float|bool|DateTimeInterface|Throwable|array $value): self
    {
        return new self($this->attributes->with($key, $value));
    }
}
