<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config;

use Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\API\Config\Cache\CacheConfigBuilder;
use Flow\ETL\API\Config\Join\HashJoinBuilder;
use Flow\ETL\API\Config\Sort\ExternalSortBuilder;
use Flow\ETL\Foundation\Clock\SystemClock;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Node\JoinAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Runtime\Analyze;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Heartbeat\Heartbeat;
use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use Flow\ETL\Runtime\Heartbeat\HeartbeatThrottle;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Runtime\Launcher\Launcher;
use Flow\ETL\Runtime\Telemetry\TelemetryConfig;
use Flow\ETL\Runtime\Telemetry\TelemetryOptions;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Path;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

use function array_map;

final class ConfigBuilder
{
    public readonly CacheConfigBuilder $cache;

    private ?Analyze $analyze;

    private ?ClockInterface $clock;

    private ?Backend $backend;

    private ?BucketingAlgorithmBuilder $groupBy;

    private ?JoinAlgorithmBuilder $join;

    private ?BucketingAlgorithmBuilder $repartition;

    /**
     * @var list<ScheduledHeartbeat>
     */
    private array $heartbeats = [];

    private bool $heartbeatsToParent = false;

    private ?string $id;

    private ?Launcher $launcher;

    private ?string $name;

    private ?Optimizer $optimizer;

    private ?Executor $executor;

    private readonly RandomValueGenerator $randomValueGenerator;

    private ?Serializer $serializer;

    private ?SortAlgorithmBuilder $sort;

    private ?TelemetryConfig $telemetryConfig;

    private readonly string $version;

    public function __construct()
    {
        $this->id = null;
        $this->name = null;
        $this->serializer = null;
        $this->backend = null;
        $this->optimizer = null;
        $this->executor = null;
        $this->clock = null;
        $this->cache = new CacheConfigBuilder();
        $this->groupBy = null;
        $this->join = null;
        $this->sort = null;
        $this->randomValueGenerator = new NativePHPRandomValueGenerator();
        $this->analyze = null;
        $this->telemetryConfig = null;
        $this->repartition = null;
        $this->launcher = null;
        $this->version = PackageVersion::get('flow-php/etl') === 'unknown'
            ? PackageVersion::get('flow-php/flow')
            : PackageVersion::get('flow-php/etl');
    }

    public function analyze(Analyze $analyze): self
    {
        $this->analyze = $analyze;

        return $this;
    }

    public function backend(Backend $backend): self
    {
        $this->backend = $backend;

        return $this;
    }

    public function build(): Config
    {
        $id = $this->id ??= 'flow-php-' . $this->randomValueGenerator->string(32);
        // built here, not in the constructor: a backend() opt-out must not depend on the default's extension check
        $backend = $this->backend ?? new AdaptiveBackend();
        $this->optimizer ??= Optimizer::default();
        $this->executor ??= new Executor();
        // built per build(), not stored: a later backend() call must reach the next build's serializer
        $serializer = $this->serializer ?? new FloeSerializer($backend);
        $optimizer = $this->optimizer;
        $executor = $this->executor;
        $dataframeName = $this->name ?? 'flow_dataframe';

        $cacheConfig = $this->cache->build($backend, $this->telemetryConfig, $dataframeName);
        $telemetryConfig = $this->telemetryConfig ?? TelemetryConfig::default($this->getClock());
        // fresh schedules per build, so two configs never share beat state
        $heartbeats = array_map(
            static fn(ScheduledHeartbeat $scheduled): ScheduledHeartbeat => new ScheduledHeartbeat(
                $scheduled->heartbeat,
                $scheduled->everySeconds,
                $scheduled->onFailure,
            ),
            $this->heartbeats,
        );

        return new Config(
            $id,
            $dataframeName,
            $this->version,
            $serializer,
            $this->getClock(),
            $optimizer,
            $executor,
            $cacheConfig,
            ($this->sort ?? new ExternalSortBuilder())->build($cacheConfig->localFilesystemCacheDir, $backend),
            $this->analyze,
            $telemetryConfig,
            ($this->groupBy ?? BucketingConfigBuilder::groupBy())->build(
                $cacheConfig->localFilesystemCacheDir,
                $backend,
            ),
            ($this->join ?? new HashJoinBuilder())->build($cacheConfig->localFilesystemCacheDir, $backend),
            ($this->repartition ?? BucketingConfigBuilder::repartition())->build(
                $cacheConfig->localFilesystemCacheDir,
                $backend,
            ),
            randomValueGenerator: $this->randomValueGenerator,
            backend: $backend,
            launcher: $this->launcher,
            heartbeat: match (true) {
                $heartbeats === [] => null,
                $this->heartbeatsToParent => HeartbeatThrottle::reportingToParent(
                    $this->getClock(),
                    $backend,
                    $telemetryConfig->telemetry->logger('flow_php_dataframe', $this->version),
                    $heartbeats,
                ),
                default => new HeartbeatThrottle(
                    $this->getClock(),
                    $backend,
                    $telemetryConfig->telemetry->logger('flow_php_dataframe', $this->version),
                    $heartbeats,
                ),
            },
        );
    }

    public function cache(RowsStorage $cache): self
    {
        $this->cache->cache($cache);

        return $this;
    }

    public function cacheDir(string|Path $dir): self
    {
        $this->cache->cacheDir($dir);

        return $this;
    }

    public function clock(ClockInterface $clocks): self
    {
        $this->clock = $clocks;

        return $this;
    }

    public function groupBy(BucketingAlgorithmBuilder $algorithm): self
    {
        $this->groupBy = $algorithm;

        return $this;
    }

    public function launcher(Launcher $launcher): self
    {
        $this->launcher = $launcher;

        return $this;
    }

    /**
     * Calls $heartbeat every $everySeconds while the run is busy; each call appends one more heartbeat.
     *
     * @throws InvalidArgumentException when $everySeconds is below 0.1, or after heartbeatToParent()
     */
    public function heartbeat(
        Heartbeat $heartbeat,
        float $everySeconds,
        HeartbeatFailure $onFailure = HeartbeatFailure::stop,
    ): self {
        if ($this->heartbeatsToParent) {
            throw new InvalidArgumentException(
                'heartbeat() and heartbeatToParent() cannot be mixed on one ConfigBuilder. A worker reports to its parent through heartbeatToParent() only. Register every other heartbeat in the parent.',
            );
        }

        $this->heartbeats[] = new ScheduledHeartbeat($heartbeat, $everySeconds, $onFailure);

        return $this;
    }

    /**
     * A failing report is logged, never thrown, because the parent ends a worker it stopped listening to.
     *
     * @throws InvalidArgumentException when $everySeconds is below 0.1, or after heartbeat()
     */
    public function heartbeatToParent(Heartbeat $heartbeat, float $everySeconds): self
    {
        if (!$this->heartbeatsToParent && $this->heartbeats !== []) {
            throw new InvalidArgumentException(
                'heartbeat() and heartbeatToParent() cannot be mixed on one ConfigBuilder. A worker reports to its parent through heartbeatToParent() only. Register every other heartbeat in the parent.',
            );
        }

        $this->heartbeats[] = new ScheduledHeartbeat($heartbeat, $everySeconds, HeartbeatFailure::log);
        $this->heartbeatsToParent = true;

        return $this;
    }

    public function id(string $id): self
    {
        $this->id = $id;

        return $this;
    }

    public function join(JoinAlgorithmBuilder $algorithm): self
    {
        $this->join = $algorithm;

        return $this;
    }

    public function name(string $name): self
    {
        $this->name = $name;

        return $this;
    }

    public function optimizer(Optimizer $optimizer): self
    {
        $this->optimizer = $optimizer;

        return $this;
    }

    public function executor(Executor $executor): self
    {
        $this->executor = $executor;

        return $this;
    }

    public function reset(): self
    {
        return new self();
    }

    public function serializer(Serializer $serializer): self
    {
        $this->serializer = $serializer;

        return $this;
    }

    public function repartition(BucketingAlgorithmBuilder $algorithm): self
    {
        $this->repartition = $algorithm;

        return $this;
    }

    public function sort(SortAlgorithmBuilder $algorithm): self
    {
        $this->sort = $algorithm;

        return $this;
    }

    public function withTelemetry(Telemetry $telemetry, TelemetryOptions $options = new TelemetryOptions()): self
    {
        $this->telemetryConfig = new TelemetryConfig($telemetry, $options);

        return $this;
    }

    private function getClock(): ClockInterface
    {
        if ($this->clock === null) {
            $this->clock = SystemClock::utc();
        }

        return $this->clock;
    }
}
