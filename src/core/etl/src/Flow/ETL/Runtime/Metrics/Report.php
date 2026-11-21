<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Values\Schema;

use function array_map;

final readonly class Report
{
    /**
     * @param null|list<SourceStatistics> $sources null when the run was not analyzed with source statistics
     */
    public function __construct(
        private ?Schema $schema,
        private Statistics $statistics,
        private ?array $sources,
        private ?string $datasetHash = null,
    ) {}

    /**
     * The JSON form for CI: the same values as the typed getters, memory in bytes.
     *
     * @return array{schema: null|array<array-key, mixed>, statistics: array<string, mixed>, sources: null|list<array<string, mixed>>, dataset_hash: null|string}
     */
    public function normalize(): array
    {
        return [
            'schema' => $this->schema?->normalize(),
            'statistics' => $this->statistics->normalize(),
            'sources' => $this->sources === null
                ? null
                : array_map(static fn(SourceStatistics $source): array => $source->normalize(), $this->sources),
            'dataset_hash' => $this->datasetHash,
        ];
    }

    /**
     * 32 lowercase hex characters; null when the run was not analyzed with a dataset hash.
     */
    public function datasetHash(): ?string
    {
        return $this->datasetHash;
    }

    /**
     * @return null|list<SourceStatistics>
     */
    public function sources(): ?array
    {
        return $this->sources;
    }

    public function schema(): ?Schema
    {
        return $this->schema;
    }

    public function statistics(): Statistics
    {
        return $this->statistics;
    }
}
