<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use function Flow\Filesystem\DSL\{fstab};
use Flow\ETL\Cache\{RowCache, RowsCache};
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\Filesystem\{Filesystem, FilesystemTable};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer, Serializer};

final class ConfigBuilder
{
    public readonly Cache\CacheConfigBuilder $cache;

    public readonly Sort\SortConfigBuilder $sort;

    private ?Caster $caster;

    private ?FilesystemTable $fstab;

    private ?string $id;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->serializer = null;
        $this->fstab = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->caster = null;
        $this->cache = new Cache\CacheConfigBuilder();
        $this->sort = new Sort\SortConfigBuilder();
    }

    public function build() : Config
    {
        $this->id ??= 'flow_php_' . bin2hex(random_bytes(16));
        $entryFactory = new NativeEntryFactory();
        $this->serializer ??= new Base64Serializer(new NativePHPSerializer());

        $this->optimizer ??= new Optimizer(
            new Optimizer\LimitOptimization(),
            new Optimizer\BatchSizeOptimization(batchSize: 1000)
        );

        $this->caster ??= Caster::default();

        return new Config(
            $this->id,
            $this->serializer,
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

    public function cache(RowsCache|RowCache $cache) : self
    {
        if ($cache instanceof RowsCache) {
            $this->cache->rowsCache($cache);
        } else {
            $this->cache->rowCache($cache);
        }

        return $this;
    }

    /**
     * @param int<1, max> $cacheBatchSize
     *
     * @throws InvalidArgumentException
     */
    public function cacheBatchSize(int $cacheBatchSize) : self
    {
        $this->cache->cacheBatchSize($cacheBatchSize);

        return $this;
    }

    public function dontPutInputIntoRows() : self
    {
        $this->putInputIntoRows = false;

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
