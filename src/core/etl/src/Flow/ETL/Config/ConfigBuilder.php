<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use Flow\Clock\SystemClock;
use Flow\ETL\Analyze;
use Flow\ETL\Cache;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\Backend;
use Flow\ETL\Config;
use Flow\ETL\Config\Cache\CacheConfigBuilder;
use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
use Flow\ETL\Config\Grouping\HashGroupByBuilder;
use Flow\ETL\Config\Join\HashJoinBuilder;
use Flow\ETL\Config\Join\JoinAlgorithmBuilder;
use Flow\ETL\Config\Repartition\HashRepartitionBuilder;
use Flow\ETL\Config\Repartition\RepartitionAlgorithmBuilder;
use Flow\ETL\Config\Sort\ExternalSortBuilder;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\Executor;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Optimizer;
use Flow\ETL\RandomValueGenerator;
use Flow\Filesystem\Path;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Serializer;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

final class ConfigBuilder
{
    public readonly CacheConfigBuilder $cache;

    private ?Analyze $analyze;

    private ?ClockInterface $clock;

    private ?Backend $backend;

    private ?GroupByAlgorithmBuilder $groupBy;

    private ?JoinAlgorithmBuilder $join;

    private ?RepartitionAlgorithmBuilder $repartition;

    private ?string $id;

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

        $cacheConfig = $this->cache->build($serializer, $this->telemetryConfig, $dataframeName);

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
            $this->telemetryConfig ?? TelemetryConfig::default($this->getClock()),
            ($this->groupBy ?? new HashGroupByBuilder())->build($cacheConfig->localFilesystemCacheDir, $backend),
            ($this->join ?? new HashJoinBuilder())->build($cacheConfig->localFilesystemCacheDir, $backend),
            ($this->repartition ?? new HashRepartitionBuilder())->build(
                $cacheConfig->localFilesystemCacheDir,
                $backend,
            ),
            randomValueGenerator: $this->randomValueGenerator,
            backend: $backend,
        );
    }

    public function cache(Cache $cache): self
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

    public function groupBy(GroupByAlgorithmBuilder $algorithm): self
    {
        $this->groupBy = $algorithm;

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

    public function repartition(RepartitionAlgorithmBuilder $algorithm): self
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
