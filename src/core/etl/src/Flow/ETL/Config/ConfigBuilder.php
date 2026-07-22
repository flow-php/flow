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
use Flow\ETL\Config\Sort\ExternalSortBuilder;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Hydrator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Serializer;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

use function Flow\Filesystem\DSL\filesystem_telemetry_config;
use function Flow\Filesystem\DSL\fstab;

final class ConfigBuilder
{
    public readonly CacheConfigBuilder $cache;

    private ?Analyze $analyze;

    private ?ClockInterface $clock;

    /**
     * @var int<1, max>
     */
    private int $extractorBatchSize;

    private ?FilesystemTable $fstab;

    private ?GroupByAlgorithmBuilder $groupBy;

    /**
     * @var null|Hydrator
     */
    private ?Hydrator $hydrator;

    private ?JoinAlgorithmBuilder $join;

    private ?string $id;

    private ?string $name;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

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
        $this->fstab = null;
        $this->hydrator = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->clock = null;
        $this->extractorBatchSize = 1000;
        $this->cache = new CacheConfigBuilder();
        $this->groupBy = null;
        $this->join = null;
        $this->sort = null;
        $this->randomValueGenerator = new NativePHPRandomValueGenerator();
        $this->analyze = null;
        $this->telemetryConfig = null;
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
        $this->optimizer ??= new Optimizer(new LimitOptimization(), new BatchSizeOptimization(batchSize: 1000));
        $this->hydrator ??= new AdaptiveRowHydrator();
        // the default serializer shares the context hydrator - one source of Row objects
        $this->serializer ??= new FloeSerializer(hydrator: $this->hydrator);

        $serializer = $this->serializer;
        $optimizer = $this->optimizer;
        $hydrator = $this->hydrator;
        $dataframeName = $this->name ?? 'flow_dataframe';

        $cacheConfig = $this->cache->build($this->fstab(), $serializer, $this->telemetryConfig, $dataframeName);

        return new Config(
            $id,
            $dataframeName,
            $this->version,
            $serializer,
            $this->getClock(),
            $this->fstab(),
            new FilesystemStreams($this->fstab()),
            $optimizer,
            $this->putInputIntoRows,
            $hydrator,
            $cacheConfig,
            ($this->sort ?? new ExternalSortBuilder())->build($this->fstab(), $cacheConfig->localFilesystemCacheDir),
            $this->analyze,
            $this->telemetryConfig ?? TelemetryConfig::default($this->getClock()),
            ($this->groupBy ?? new HashGroupByBuilder())->build($this->fstab(), $cacheConfig->localFilesystemCacheDir),
            ($this->join ?? new HashJoinBuilder())->build($this->fstab(), $cacheConfig->localFilesystemCacheDir),
            $this->extractorBatchSize,
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

    public function cacheFilesystem(string $protocol): self
    {
        $this->cache->filesystemMount($protocol);

        return $this;
    }

    public function clock(ClockInterface $clocks): self
    {
        $this->clock = $clocks;

        return $this;
    }

    /**
     * Number of rows a streaming extractor buffers before hydrating them in one batch.
     *
     * @param int<1, max> $extractorBatchSize
     */
    public function extractorBatchSize(int $extractorBatchSize): self
    {
        $this->extractorBatchSize = $extractorBatchSize;

        return $this;
    }

    public function dontPutInputIntoRows(): self
    {
        $this->putInputIntoRows = false;

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

    public function mount(Filesystem $filesystem): self
    {
        $this->fstab()->mount($filesystem);

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

    /**
     * When set, each extractor will try to put additional rows with input parameters, like for example uri to the source file from which
     * data is extracted.
     */
    public function putInputIntoRows(): self
    {
        $this->putInputIntoRows = true;

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

    public function sort(SortAlgorithmBuilder $algorithm): self
    {
        $this->sort = $algorithm;

        return $this;
    }

    public function unmount(Filesystem $filesystem): self
    {
        $this->fstab()->unmount($filesystem);

        return $this;
    }

    public function withTelemetry(Telemetry $telemetry, TelemetryOptions $options = new TelemetryOptions()): self
    {
        $this->telemetryConfig = new TelemetryConfig($telemetry, $options);

        if ($this->fstab !== null) {
            $this->fstab->withTelemetry(filesystem_telemetry_config(
                $telemetry,
                $this->getClock(),
                $options->filesystem,
            ));
        }

        return $this;
    }

    private function fstab(): FilesystemTable
    {
        if ($this->fstab === null) {
            $this->fstab = fstab();

            $filesystemOptions = $this->telemetryConfig?->options->filesystem;

            if (
                $filesystemOptions !== null
                && ($filesystemOptions->traceStreams || $filesystemOptions->collectMetrics)
            ) {
                $this->fstab->withTelemetry(filesystem_telemetry_config(
                    $this->telemetry()->telemetry,
                    $this->getClock(),
                    $filesystemOptions,
                ));
            }
        }

        return $this->fstab;
    }

    private function getClock(): ClockInterface
    {
        if ($this->clock === null) {
            $this->clock = SystemClock::utc();
        }

        return $this->clock;
    }

    private function telemetry(): TelemetryConfig
    {
        if ($this->telemetryConfig === null) {
            $this->telemetryConfig = TelemetryConfig::default($this->getClock());
        }

        return $this->telemetryConfig;
    }
}
