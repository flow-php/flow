<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use Flow\Clock\SystemClock;
use Flow\ETL\Analyze;
use Flow\ETL\Cache;
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
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Hydrator;
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

    private ?GroupByAlgorithmBuilder $groupBy;

    /**
     * @var null|Hydrator
     */
    private ?Hydrator $hydrator;

    private ?JoinAlgorithmBuilder $join;

    private ?RepartitionAlgorithmBuilder $repartition;

    private ?string $id;

    private ?string $name;

    private ?Optimizer $optimizer;

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
        $this->hydrator = null;
        $this->optimizer = null;
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

    public function build(): Config
    {
        $id = $this->id ??= 'flow-php-' . $this->randomValueGenerator->string(32);
        $this->optimizer ??= new Optimizer(new LimitOptimization());
        $this->hydrator ??= new AdaptiveRowHydrator();
        // the default serializer shares the context hydrator - one source of Row objects
        $this->serializer ??= new FloeSerializer(hydrator: $this->hydrator);

        $serializer = $this->serializer;
        $optimizer = $this->optimizer;
        $hydrator = $this->hydrator;
        $dataframeName = $this->name ?? 'flow_dataframe';

        $cacheConfig = $this->cache->build($serializer, $this->telemetryConfig, $dataframeName);

        return new Config(
            $id,
            $dataframeName,
            $this->version,
            $serializer,
            $this->getClock(),
            $optimizer,
            $hydrator,
            $cacheConfig,
            ($this->sort ?? new ExternalSortBuilder())->build($cacheConfig->localFilesystemCacheDir),
            $this->analyze,
            $this->telemetryConfig ?? TelemetryConfig::default($this->getClock()),
            ($this->groupBy ?? new HashGroupByBuilder())->build($cacheConfig->localFilesystemCacheDir),
            ($this->join ?? new HashJoinBuilder())->build($cacheConfig->localFilesystemCacheDir),
            ($this->repartition ?? new HashRepartitionBuilder())->build($cacheConfig->localFilesystemCacheDir),
            randomValueGenerator: $this->randomValueGenerator,
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

    /**
     * @param Hydrator $hydrator
     */
    public function hydrator(Hydrator $hydrator): self
    {
        $this->hydrator = $hydrator;

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
