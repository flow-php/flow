<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, fstab};
use Flow\Clock\SystemClock;
use Flow\ETL\{Analyze, Cache, Config, NativePHPRandomValueGenerator, RandomValueGenerator};
use Flow\ETL\Config\Cache\CacheConfigBuilder;
use Flow\ETL\Config\Sort\SortConfigBuilder;
use Flow\ETL\Config\Telemetry\{TelemetryConfig, TelemetryOptions};
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\{BatchSizeOptimization, LimitOptimization};
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\{Filesystem, FilesystemTable};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer, Serializer};
use Flow\Telemetry\{PackageVersion, Telemetry};
use Psr\Clock\ClockInterface;

final class ConfigBuilder
{
    public readonly CacheConfigBuilder $cache;

    public readonly SortConfigBuilder $sort;

    private ?Analyze $analyze;

    private ?ClockInterface $clock;

    private ?FilesystemTable $fstab;

    private ?string $id;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private readonly RandomValueGenerator $randomValueGenerator;

    private ?Serializer $serializer;

    private ?TelemetryConfig $telemetryConfig;

    private readonly string $version;

    public function __construct()
    {
        $this->id = null;
        $this->serializer = null;
        $this->fstab = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->clock = null;
        $this->cache = new CacheConfigBuilder();
        $this->sort = new SortConfigBuilder();
        $this->randomValueGenerator = new NativePHPRandomValueGenerator();
        $this->analyze = null;
        $this->telemetryConfig = null;
        $this->version = PackageVersion::get('flow-php/etl') === 'unknown' ? PackageVersion::get('flow-php/flow') : PackageVersion::get('flow-php/etl');
    }

    public function analyze(Analyze $analyze) : self
    {
        $this->analyze = $analyze;

        return $this;
    }

    public function build(EntryFactory $entryFactory = new EntryFactory()) : Config
    {
        $this->id ??= 'flow_php' . $this->randomValueGenerator->string(32);
        $this->serializer ??= new Base64Serializer(new NativePHPSerializer());
        $this->clock ??= SystemClock::utc();
        $this->optimizer ??= new Optimizer(
            new LimitOptimization(),
            new BatchSizeOptimization(batchSize: 1000)
        );

        return new Config(
            $this->id,
            $this->version,
            $this->serializer,
            $this->clock,
            $this->fstab(),
            new FilesystemStreams($this->fstab()),
            $this->optimizer,
            $this->putInputIntoRows,
            $entryFactory,
            $this->cache->build($this->fstab(), $this->serializer),
            $this->sort->build(),
            $this->analyze,
            $this->telemetryConfig ?? TelemetryConfig::default($this->clock),
        );
    }

    public function cache(Cache $cache) : self
    {
        $this->cache->cache($cache);

        return $this;
    }

    public function clock(ClockInterface $clocks) : self
    {
        $this->clock = $clocks;

        return $this;
    }

    public function dontPutInputIntoRows() : self
    {
        $this->putInputIntoRows = false;

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBucketsCount
     */
    public function externalSortBucketsCount(int $externalSortBucketsCount) : self
    {
        $this->cache->externalSortBucketsCount($externalSortBucketsCount);

        return $this;
    }

    public function id(string $id) : self
    {
        $this->id = $id;

        return $this;
    }

    public function mount(Filesystem $filesystem) : self
    {
        $this->fstab()->mount($filesystem);

        return $this;
    }

    public function optimizer(Optimizer $optimizer) : self
    {
        $this->optimizer = $optimizer;

        return $this;
    }

    /**
     * When set, each extractor will try to put additional rows with input parameters, like for example uri to the source file from which
     * data is extracted.
     */
    public function putInputIntoRows() : self
    {
        $this->putInputIntoRows = true;

        return $this;
    }

    public function reset() : self
    {
        return new self();
    }

    public function serializer(Serializer $serializer) : self
    {
        $this->serializer = $serializer;

        return $this;
    }

    public function sortMemoryLimit(Unit $unit) : self
    {
        $this->sort->sortMemoryLimit($unit);

        return $this;
    }

    public function unmount(Filesystem $filesystem) : self
    {
        $this->fstab()->unmount($filesystem);

        return $this;
    }

    public function withTelemetry(Telemetry $telemetry, TelemetryOptions $options = new TelemetryOptions()) : self
    {
        $this->telemetryConfig = new TelemetryConfig($telemetry, $options);

        if ($this->fstab !== null) {
            $this->fstab->withTelemetry(
                filesystem_telemetry_config(
                    $telemetry,
                    $this->clock ?? SystemClock::utc(),
                    $options->filesystem
                )
            );
        }

        return $this;
    }

    private function fstab() : FilesystemTable
    {
        if ($this->fstab === null) {
            $this->fstab = fstab();

            $filesystemOptions = $this->telemetryConfig?->options->filesystem;

            if ($filesystemOptions !== null && ($filesystemOptions->traceStreams || $filesystemOptions->collectMetrics)) {
                $this->fstab->withTelemetry(filesystem_telemetry_config(
                    $this->telemetry()->telemetry,
                    $this->clock ?? SystemClock::utc(),
                    $filesystemOptions
                ));
            }
        }

        return $this->fstab;
    }

    private function telemetry() : TelemetryConfig
    {
        if ($this->telemetryConfig === null) {
            $this->telemetryConfig = TelemetryConfig::default($this->clock ?? SystemClock::utc());
        }

        return $this->telemetryConfig;
    }
}
