<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use Flow\Clock\SystemClock;
use Flow\ETL\Analyze;
use Flow\ETL\Cache;
use Flow\ETL\Config;
use Flow\ETL\Config\Cache\CacheConfigBuilder;
use Flow\ETL\Config\Grouping\GroupingConfigBuilder;
use Flow\ETL\Config\Sort\SortConfigBuilder;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\FilesystemTable;
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

    public readonly GroupingConfigBuilder $grouping;

    public readonly SortConfigBuilder $sort;

    private ?Analyze $analyze;

    private ?ClockInterface $clock;

    private ?FilesystemTable $fstab;

    private ?string $id;

    private ?string $name;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private readonly RandomValueGenerator $randomValueGenerator;

    private ?Serializer $serializer;

    private ?TelemetryConfig $telemetryConfig;

    private readonly string $version;

    public function __construct()
    {
        $this->id = null;
        $this->name = null;
        $this->serializer = null;
        $this->fstab = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->clock = null;
        $this->cache = new CacheConfigBuilder();
        $this->grouping = new GroupingConfigBuilder();
        $this->sort = new SortConfigBuilder();
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

    public function build(EntryFactory $entryFactory = new EntryFactory()): Config
    {
        $id = $this->id ??= 'flow-php-' . $this->randomValueGenerator->string(32);
        $this->serializer ??= new FloeSerializer();
        $this->optimizer ??= new Optimizer(new LimitOptimization(), new BatchSizeOptimization(batchSize: 1000));

        $serializer = $this->serializer;
        $optimizer = $this->optimizer;
        $dataframeName = $this->name ?? 'flow_dataframe';

        $cacheConfig = $this->cache->build($this->fstab(), $this->telemetryConfig, $dataframeName);

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
            $entryFactory,
            $cacheConfig,
            $this->sort->build(),
            $this->analyze,
            $this->telemetryConfig ?? TelemetryConfig::default($this->getClock()),
            $this->grouping->build($this->fstab(), $cacheConfig->localFilesystemCacheDir),
        );
    }

    public function cache(Cache $cache): self
    {
        $this->cache->cache($cache);

        return $this;
    }

    public function cacheFilesystem(string $protocol): self
    {
        $this->cache->filesystemMount($protocol);

        return $this;
    }

    /**
     * @param int<1, max> $serializerBatchSize
     */
    public function cacheSerializerBatchSize(int $serializerBatchSize): self
    {
        $this->cache->serializerBatchSize($serializerBatchSize);

        return $this;
    }

    public function clock(ClockInterface $clocks): self
    {
        $this->clock = $clocks;

        return $this;
    }

    public function dontPutInputIntoRows(): self
    {
        $this->putInputIntoRows = false;

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBucketsCount
     */
    public function externalSortBucketsCount(int $externalSortBucketsCount): self
    {
        $this->cache->externalSortBucketsCount($externalSortBucketsCount);

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBatchSize
     */
    public function externalSortBatchSize(int $externalSortBatchSize): self
    {
        $this->cache->externalSortBatchSize($externalSortBatchSize);

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBucketSize
     */
    public function externalSortBucketSize(int $externalSortBucketSize): self
    {
        $this->cache->externalSortBucketSize($externalSortBucketSize);

        return $this;
    }

    public function externalSortFilesystem(string $protocol): self
    {
        $this->sort->filesystemProtocol($protocol);

        return $this;
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function groupingBatchSize(int $batchSize): self
    {
        $this->grouping->batchSize($batchSize);

        return $this;
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public function groupingBucketsCount(int $bucketsCount): self
    {
        $this->grouping->bucketsCount($bucketsCount);

        return $this;
    }

    public function groupingCache(BucketsCache $cache): self
    {
        $this->grouping->cache($cache);

        return $this;
    }

    public function id(string $id): self
    {
        $this->id = $id;

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
