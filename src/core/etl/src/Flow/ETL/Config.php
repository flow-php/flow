<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Calculator\Calculator;
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Config\Grouping\GroupingConfig;
use Flow\ETL\Config\Join\JoinConfig;
use Flow\ETL\Config\Sort\SortConfig;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Hydrator;
use Flow\Filesystem\FilesystemTable;
use Flow\Serializer\Serializer;
use Psr\Clock\ClockInterface;

/**
 * Immutable configuration that can be used to initialize many contexts.
 * Configuration must not be changed after it's passed to FlowContext.
 */
final readonly class Config
{
    /**
     * @param Hydrator $hydrator
     * @param int<1, max> $extractorBatchSize
     */
    public function __construct(
        private string $id,
        private string $name,
        private string $version,
        private Serializer $serializer,
        private ClockInterface $clock,
        private FilesystemTable $filesystemTable,
        private FilesystemStreams $filesystemStreams,
        private Optimizer $optimizer,
        private bool $putInputIntoRows,
        private Hydrator $hydrator,
        public CacheConfig $cache,
        public SortConfig $sort,
        private ?Analyze $analyze,
        public TelemetryConfig $telemetry,
        public GroupingConfig $grouping,
        public JoinConfig $join,
        private int $extractorBatchSize = 1000,
        private EntryFactory $entryFactory = new EntryFactory(),
        private Calculator $calculator = new Calculator(),
    ) {}

    public static function builder(): ConfigBuilder
    {
        return new ConfigBuilder();
    }

    public static function default(): self
    {
        return self::builder()->build();
    }

    public function analyze(): ?Analyze
    {
        return $this->analyze;
    }

    public function calculator(): Calculator
    {
        return $this->calculator;
    }

    public function clock(): ClockInterface
    {
        return $this->clock;
    }

    public function filesystemStreams(): FilesystemStreams
    {
        return $this->filesystemStreams;
    }

    public function fstab(): FilesystemTable
    {
        return $this->filesystemTable;
    }

    public function entryFactory(): EntryFactory
    {
        return $this->entryFactory;
    }

    /**
     * @return int<1, max>
     */
    public function extractorBatchSize(): int
    {
        return $this->extractorBatchSize;
    }

    /**
     * @return Hydrator
     */
    public function hydrator(): Hydrator
    {
        return $this->hydrator;
    }

    public function id(): string
    {
        return $this->id;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function optimizer(): Optimizer
    {
        return $this->optimizer;
    }

    public function serializer(): Serializer
    {
        return $this->serializer;
    }

    public function shouldPutInputIntoRows(): bool
    {
        return $this->putInputIntoRows;
    }

    public function version(): string
    {
        return $this->version;
    }
}
