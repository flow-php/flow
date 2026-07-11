<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;
use JsonException;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function json_decode;
use function json_encode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final readonly class Footer
{
    /**
     * @param array<int, array<int, array<string, mixed>>> $schemas decoded SCHEMA frame bodies, index = schemaId
     * @param array<int, array<string, mixed>> $fileSchema normalized merged schema
     * @param array<int, Section> $sections
     * @param array<string, string> $partitions
     */
    public function __construct(
        public int $version,
        public string $writer,
        public array $schemas,
        public array $fileSchema,
        public array $sections,
        public array $partitions,
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

        try {
            $data = type_structure([
                'version' => type_integer(),
                'writer' => type_string(),
                'schemas' => type_array(),
                'fileSchema' => type_array(),
                'sections' => type_list(type_array()),
                'partitions' => type_array(),
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

        /** @var array<int, array<int, array<string, mixed>>> $schemas */
        $schemas = $data['schemas'];
        /** @var array<int, array<string, mixed>> $fileSchema */
        $fileSchema = $data['fileSchema'];
        /** @var array<string, string> $partitions */
        $partitions = $data['partitions'];

        try {
            /** @var array<string, array<array-key, mixed>|bool|float|int|string> $rawMetadata */
            $rawMetadata = $data['metadata'];
            $metadata = Metadata::fromArray($rawMetadata);
        } catch (InvalidArgumentException $e) {
            throw new FloeException('Floe footer metadata is malformed: ' . $e->getMessage(), 0, $e);
        }

        return new self(
            $data['version'],
            $data['writer'],
            $schemas,
            $fileSchema,
            $sections,
            $partitions,
            $data['totalRows'],
            $metadata,
        );
    }

    public function fileSchema(): Schema
    {
        return Schema::fromArray($this->fileSchema);
    }

    /**
     * @throws FloeException
     */
    public function schemaBody(int $schemaId): string
    {
        if (!array_key_exists($schemaId, $this->schemas)) {
            throw new FloeException(sprintf('Floe footer does not hold schema with id %d', $schemaId));
        }

        return json_encode($this->schemas[$schemaId], JSON_THROW_ON_ERROR);
    }

    public function toJson(): string
    {
        $sections = [];

        foreach ($this->sections as $section) {
            $sections[] = $section->normalize();
        }

        return json_encode([
            'version' => $this->version,
            'writer' => $this->writer,
            'schemas' => $this->schemas,
            'fileSchema' => $this->fileSchema,
            'sections' => $sections,
            'partitions' => (object) $this->partitions,
            'totalRows' => $this->totalRows,
            'metadata' => (object) $this->metadata->normalize(),
        ], JSON_THROW_ON_ERROR);
    }
}
