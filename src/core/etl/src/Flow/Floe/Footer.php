<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;
use JsonException;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_array;
use function json_decode;
use function json_encode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final readonly class Footer
{
    /**
     * @param array<int, array<string, mixed>> $schema normalized file schema (a file carries exactly one)
     * @param array<int, Section> $sections
     * @param array<int, array<string, string>> $partitions deduped PARTITIONS frame combinations, index = partitionsId
     */
    public function __construct(
        public int $version,
        public string $writer,
        public array $schema,
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
                'partitions' => type_list(type_array()),
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
        /** @var array<int, array<string, string>> $partitions */
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
            $schema,
            $sections,
            $partitions,
            $data['totalRows'],
            $metadata,
        );
    }

    public function schema(): Schema
    {
        return Schema::fromArray($this->schema);
    }

    /**
     * The single combination of a single-combination file, in the order it was
     * written (the PARTITIONS frame / table entry preserves it). A zero-section
     * value keeps only its combination in the table, so it is recovered from the
     * last non-empty table entry.
     *
     * @throws FloeException
     *
     * @return array<int, Partition>
     */
    public function filePartitions(): array
    {
        if ($this->sections !== []) {
            $combo = $this->partitionsFor($this->sections[0]->partitionsId);
        } else {
            $combo = [];

            foreach ($this->partitions as $entry) {
                if ($entry !== []) {
                    $combo = $entry;
                }
            }
        }

        $partitions = [];

        foreach ($combo as $name => $value) {
            $partitions[] = new Partition($name, $value);
        }

        return $partitions;
    }

    /**
     * Rebuilds Rows from already-decoded (un-partitioned) rows, reattaching the
     * file's single partition combination in its original order.
     *
     * @param array<int, Row> $rows
     *
     * @throws FloeException
     */
    public function reconstructRows(array $rows): Rows
    {
        $partitions = $this->filePartitions();

        return $partitions === [] ? new Rows(...$rows) : Rows::partitioned($rows, $partitions);
    }

    public function schemaBody(): string
    {
        return json_encode($this->schema, JSON_THROW_ON_ERROR);
    }

    /**
     * @throws FloeException
     *
     * @return array<string, string>
     */
    public function partitionsFor(int $partitionsId): array
    {
        if (!array_key_exists($partitionsId, $this->partitions)) {
            throw new FloeException(sprintf('Floe footer does not hold partitions with id %d', $partitionsId));
        }

        return $this->partitions[$partitionsId];
    }

    /**
     * @return array{
     *     version: int,
     *     writer: string,
     *     schema: array<int, array<string, mixed>>,
     *     sections: array<int, array{offset: int, partitionsId: int, rowCount: int}>,
     *     partitions: array<int, array<string, string>>,
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
            'partitions' => $this->partitions,
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

        return json_encode($data, JSON_THROW_ON_ERROR);
    }
}
