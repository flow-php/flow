<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Runtime\Executor\OutputHasher;
use Flow\ETL\Runtime\Executor\PartitionedOutputHasher;
use Flow\ETL\Runtime\Executor\WholeOutputHasher;

final class Analyze
{
    private bool $collectColumnStatistics = false;

    private bool $collectSchema = false;

    private bool $collectSourceStatistics = false;

    private ?References $datasetHashBy = null;

    private HashAlgorithm $datasetHashAlgorithm;

    public function __construct() {}

    public function collectColumnStatistics(): bool
    {
        return $this->collectColumnStatistics;
    }

    public function collectSchema(): bool
    {
        return $this->collectSchema;
    }

    public function collectSourceStatistics(): bool
    {
        return $this->collectSourceStatistics;
    }

    public function withColumnStatistics(): self
    {
        $this->collectColumnStatistics = true;

        return $this;
    }

    public function newOutputHasher(): ?OutputHasher
    {
        return match (true) {
            $this->datasetHashBy === null => null,
            $this->datasetHashBy->count() === 0 => new WholeOutputHasher($this->datasetHashAlgorithm),
            default => new PartitionedOutputHasher($this->datasetHashBy, $this->datasetHashAlgorithm),
        };
    }

    /**
     * @param array<Reference|string> $partitions
     */
    public function withDatasetHash(array $partitions = [], HashAlgorithm $algorithm = HashAlgorithm::xxh128): self
    {
        $this->datasetHashBy = new References(...$partitions);
        $this->datasetHashAlgorithm = $algorithm;

        return $this;
    }

    public function withSchema(): self
    {
        $this->collectSchema = true;

        return $this;
    }

    public function withSourceStatistics(): self
    {
        $this->collectSourceStatistics = true;

        return $this;
    }
}
