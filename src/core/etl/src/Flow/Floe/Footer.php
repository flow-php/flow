<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;
use JsonException;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_array;
use function json_decode;
use function json_encode;

use const JSON_THROW_ON_ERROR;

final readonly class Footer
{
    /**
     * @param array<int, array<string, mixed>> $schema normalized file schema (a file carries exactly one)
     * @param array<int, Section> $sections
     */
    public function __construct(
        public int $version,
        public string $writer,
        public array $schema,
        public array $sections,
        public int $totalRows,
        public Metadata $metadata,
    ) {}

    /**
     * @throws FloeException
     */
    public static function fromJson(string $json): self
    {
        try {
            // @mago-ignore analysis:mixed-assignment
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to decode footer JSON: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($data)) {
            throw new FloeException('Floe footer is malformed: footer JSON is not an object');
        }

        return self::fromArray($data);
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws FloeException
     */
    public static function fromArray(array $data): self
    {
        try {
            $data = type_structure([
                'version' => type_integer(),
                'writer' => type_string(),
                'schema' => type_array(),
                'sections' => type_list(type_array()),
                'totalRows' => type_integer(),
                'metadata' => type_array(),
            ])->assert($data);
        } catch (InvalidTypeException $e) {
            throw new FloeException('Floe footer is malformed: ' . $e->getMessage(), 0, $e);
        }

        $sections = [];

        /** @var array<int, array<string, mixed>> $sectionsData */
        $sectionsData = $data['sections'];

        foreach ($sectionsData as $section) {
            $sections[] = Section::fromArray($section);
        }

        /** @var array<int, array<string, mixed>> $schema */
        $schema = $data['schema'];
        try {
            /** @var array<string, array<array-key, mixed>|bool|float|int|string> $rawMetadata */
            $rawMetadata = $data['metadata'];
            $metadata = Metadata::fromArray($rawMetadata);
        } catch (InvalidArgumentException $e) {
            throw new FloeException('Floe footer metadata is malformed: ' . $e->getMessage(), 0, $e);
        }

        return new self($data['version'], $data['writer'], $schema, $sections, $data['totalRows'], $metadata);
    }

    public function schema(): Schema
    {
        return Schema::fromArray($this->schema);
    }

    public function schemaBody(): string
    {
        return json_encode($this->schema, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array{
     *     version: int,
     *     writer: string,
     *     schema: array<int, array<string, mixed>>,
     *     sections: array<int, array{offset: int, rowCount: int}>,
     *     totalRows: int,
     *     metadata: array<string, mixed>,
     * }
     */
    public function normalize(): array
    {
        $sections = [];

        foreach ($this->sections as $section) {
            $sections[] = $section->normalize();
        }

        return [
            'version' => $this->version,
            'writer' => $this->writer,
            'schema' => $this->schema,
            'sections' => $sections,
            'totalRows' => $this->totalRows,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    /**
     * @throws JsonException
     */
    public function toJson(): string
    {
        $data = $this->normalize();
        // metadata is a map: an empty one must encode as a JSON object ({}), not a list ([])
        $data['metadata'] = (object) $data['metadata'];

        try {
            return json_encode($data, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to encode schema as JSON: ' . $e->getMessage(), 0, $e);
        }
    }
}
