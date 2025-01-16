<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use function Flow\Filesystem\DSL\fstab;
use Flow\Clock\SystemClock;
use Flow\ETL\Config\Cache\CacheConfigBuilder;
use Flow\ETL\Config\Sort\SortConfigBuilder;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\{Cache, Config, NativePHPRandomValueGenerator, RandomValueGenerator};
use Flow\Filesystem\{Filesystem, FilesystemTable};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer, Serializer};
use Psr\Clock\ClockInterface;

final class ConfigBuilder
{
    public readonly CacheConfigBuilder $cache;

    public readonly SortConfigBuilder $sort;

    private ?Caster $caster;

    private ?ClockInterface $clock;

    private ?FilesystemTable $fstab;

    private ?string $id;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private readonly RandomValueGenerator $randomValueGenerator;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->serializer = null;
        $this->fstab = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->caster = null;
        $this->clock = null;
        $this->cache = new CacheConfigBuilder();
        $this->sort = new SortConfigBuilder();
        $this->randomValueGenerator = new NativePHPRandomValueGenerator();
    }

    public function build() : Config
    {
        $this->id ??= 'flow_php' . $this->randomValueGenerator->string(32);
        $entryFactory = new NativeEntryFactory();
        $this->serializer ??= new Base64Serializer(new NativePHPSerializer());
        $this->clock ??= SystemClock::utc();
        $this->optimizer ??= new Optimizer(
            new Optimizer\LimitOptimization(),
            new Optimizer\BatchSizeOptimization(batchSize: 1000)
        );

        $this->caster ??= Caster::default();

        return new Config(
            $this->id,
            $this->serializer,
            $this->clock,
            $this->fstab(),
            new FilesystemStreams($this->fstab()),
            $this->optimizer,
            $this->caster,
            $this->putInputIntoRows,
            $entryFactory,
            $this->cache->build($this->fstab(), $this->serializer),
            $this->sort->build()
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

    private function fstab() : FilesystemTable
    {
        if ($this->fstab === null) {
            $this->fstab = fstab();
        }

        return $this->fstab;
    }
}
