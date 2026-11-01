<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * @see https://opentelemetry.io/docs/specs/otel/glossary/#instrumentation-scope
 *
 * @import-type TAttributeValue from Attributes
 * @import-type TAttributeValueMap from Attributes
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
     * @param array{name: string, version?: string, schemaUrl?: null|string, attributes?: array<string, mixed>} $data
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
     * @return array{name: string, version: string, schemaUrl: null|string, attributes: array<string, mixed>}
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
     * @param TAttributeValueMap|Attributes $attributes
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
