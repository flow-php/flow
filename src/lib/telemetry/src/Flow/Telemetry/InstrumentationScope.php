<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Represents the instrumentation scope (library) producing telemetry.
 *
 * InstrumentationScope identifies the library or application component
 * that is generating telemetry data. This information is attached to
 * all telemetry (spans, logs, metrics) for attribution and filtering.
 *
 * Example usage:
 * ```php
 * $scope = new InstrumentationScope(
 *     name: 'my-service',
 *     version: '1.2.3',
 *     schemaUrl: 'https://opentelemetry.io/schemas/1.21.0',
 * );
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/glossary/#instrumentation-scope
 */
final readonly class InstrumentationScope
{
    /**
     * @param string $name The name of the instrumentation scope (typically library or service name)
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl The schema URL for semantic conventions used by this scope
     * @param Attributes $attributes Additional scope attributes
     */
    public function __construct(
        public string $name,
        public string $version = 'unknown',
        public ?string $schemaUrl = null,
        public Attributes $attributes = new Attributes(),
    ) {}

    /**
     * Create an InstrumentationScope from a normalized array representation.
     *
     * @param array{name: string, version?: string, schemaUrl?: null|string, attributes?: array<string, array<bool|float|int|string>|bool|float|int|string>} $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['version'] ?? 'unknown',
            $data['schemaUrl'] ?? null,
            Attributes::fromArray($data['attributes'] ?? []),
        );
    }

    /**
     * Normalize the scope to an array representation for serialization.
     *
     * @return array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'version' => $this->version,
            'schemaUrl' => $this->schemaUrl,
            'attributes' => $this->attributes->normalize(),
        ];
    }

    /**
     * Create a new scope with additional attributes merged with existing ones.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes
     */
    public function withAttributes(Attributes|array $attributes): self
    {
        $attrs = $attributes instanceof Attributes ? $attributes : Attributes::create($attributes);

        return new self($this->name, $this->version, $this->schemaUrl, $this->attributes->merge($attrs));
    }

    /**
     * Create a new scope with a specific schema URL.
     */
    public function withSchemaUrl(string $schemaUrl): self
    {
        return new self($this->name, $this->version, $schemaUrl, $this->attributes);
    }
}
