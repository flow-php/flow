<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Calculator\Calculator;
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Config\Grouping\HashGroupByConfig;
use Flow\ETL\Config\Join\HashJoinConfig;
use Flow\ETL\Config\Repartition\HashRepartitionConfig;
use Flow\ETL\Config\Sort\ExternalSortConfig;
use Flow\ETL\Config\Sort\MemorySortConfig;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Hydrator;
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
     */
    public function __construct(
        private string $id,
        private string $name,
        private string $version,
        private Serializer $serializer,
        private ClockInterface $clock,
        private Optimizer $optimizer,
        private Hydrator $hydrator,
        public CacheConfig $cache,
        public MemorySortConfig|ExternalSortConfig $sort,
        private ?Analyze $analyze,
        public TelemetryConfig $telemetry,
        public HashGroupByConfig $grouping,
        public HashJoinConfig $join,
        public HashRepartitionConfig $repartition,
        private Calculator $calculator = new Calculator(),
        private RandomValueGenerator $randomValueGenerator = new NativePHPRandomValueGenerator(),
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

    public function randomValueGenerator(): RandomValueGenerator
    {
        return $this->randomValueGenerator;
    }

    public function serializer(): Serializer
    {
        return $this->serializer;
    }

    public function version(): string
    {
        return $this->version;
    }
}
