<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Planning\Physical\NodeTranslator;
use Flow\ETL\Planning\Physical\Planner;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Launcher\Launcher;
use Flow\ETL\Runtime\Telemetry\TelemetryConfig;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Column\Backend;
use Psr\Clock\ClockInterface;

use function array_map;

/**
 * Immutable configuration that can be used to initialize many contexts.
 * Configuration must not be changed after it's passed to FlowContext.
 */
final readonly class Config
{
    private Planner $planner;

    public function __construct(
        private string $id,
        private string $name,
        private string $version,
        private Serializer $serializer,
        private ClockInterface $clock,
        private Optimizer $optimizer,
        private Executor $executor,
        public CacheConfig $cache,
        public MemorySortConfig|ExternalSortConfig $sort,
        private ?Analyze $analyze,
        public TelemetryConfig $telemetry,
        public BucketingConfig $grouping,
        public HashJoinConfig $join,
        public BucketingConfig $repartition,
        private Backend $backend,
        private RandomValueGenerator $randomValueGenerator = new NativePHPRandomValueGenerator(),
        private ?Launcher $launcher = null,
    ) {
        $this->planner = new Planner(
            $optimizer,
            new NodeTranslator(
                $backend,
                $randomValueGenerator,
                $id,
                $cache->cache,
                $cache->localFilesystemCacheDir,
                $sort,
                $grouping,
                $join,
                $repartition,
            ),
            $backend,
        );
    }

    public function analyze(): ?Analyze
    {
        return $this->analyze;
    }

    public function backend(): Backend
    {
        return $this->backend;
    }

    /**
     * Runs every read of a SplittableExtractor in this run: one launcher serves the whole run. Null: every read runs in
     * this process.
     */
    public function launcher(): ?Launcher
    {
        return $this->launcher;
    }

    public function clock(): ClockInterface
    {
        return $this->clock;
    }

    public function executor(): Executor
    {
        return $this->executor;
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

    public function planner(): Planner
    {
        return $this->planner;
    }

    public function randomValueGenerator(): RandomValueGenerator
    {
        return $this->randomValueGenerator;
    }

    public function serializer(): Serializer
    {
        return $this->serializer;
    }

    /**
     * The settings a run logs when it starts.
     *
     * @return array{cache: class-string, serializer: class-string, optimizer_rules: list<class-string<Rule>>, telemetry: array{trace_loading: bool, trace_transformations: bool, collect_metrics: bool}, spill: array{sort: class-string, group_by: class-string, join: class-string}}
     */
    public function telemetryAttributes(): array
    {
        return [
            'cache' => $this->cache->cache::class,
            'serializer' => $this->serializer::class,
            'optimizer_rules' => array_map(static fn(Rule $rule): string => $rule::class, $this->optimizer->rules()),
            'telemetry' => [
                'trace_loading' => $this->telemetry->options->traceLoading,
                'trace_transformations' => $this->telemetry->options->traceTransformations,
                'collect_metrics' => $this->telemetry->options->collectMetrics,
            ],
            'spill' => [
                'sort' => $this->sort instanceof ExternalSortConfig
                    ? $this->sort->bucketing->storage::class
                    : MemorySortConfig::class,
                'group_by' => $this->grouping->storage::class,
                'join' => $this->join->bucketing->storage::class,
            ],
        ];
    }

    public function version(): string
    {
        return $this->version;
    }
}
