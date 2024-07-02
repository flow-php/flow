<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Filesystem\{FilesystemStreams};
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\{Filesystem, FilesystemTable};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer, Serializer};

final class ConfigBuilder
{
    private ?Cache $cache;

    /**
     * @var int<1, max>
     */
    private int $cacheBatchSize = 1000;

    private ?Caster $caster;

    private ?ExternalSort $externalSort;

    private ?FilesystemTable $fstab;

    private ?string $id;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->serializer = null;
        $this->cache = null;
        $this->externalSort = null;
        $this->fstab = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
        $this->caster = null;
    }

    /**
     * @psalm-suppress  PossiblyFalseArgument
     */
    public function build() : Config
    {
        $this->id ??= 'flow_php' . bin2hex(random_bytes(16));
        $entryFactory = new NativeEntryFactory();
        $this->serializer ??= new Base64Serializer(new NativePHPSerializer());
        $cachePath = \is_string(\getenv(Config::CACHE_DIR_ENV)) && \realpath(\getenv(Config::CACHE_DIR_ENV))
            ? \getenv(Config::CACHE_DIR_ENV)
            : \sys_get_temp_dir() . '/flow_php/cache';

        if ($this->cache === null) {
            if (!\file_exists($cachePath)) {
                \mkdir($cachePath, 0777, true);
            }

            $this->cache = new LocalFilesystemCache($cachePath, $this->serializer);
        }

        $this->externalSort ??= new MemorySort(
            $this->id,
            $this->cache,
            \is_string(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) ? Unit::fromString(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) : Unit::fromMb(200)
        );

        $this->optimizer ??= new Optimizer(
            new Optimizer\LimitOptimization(),
            new Optimizer\BatchSizeOptimization(batchSize: 1000)
        );

        $this->caster ??= Caster::default();

        return new Config(
            $this->id,
            $this->serializer,
            $this->cache,
            $this->externalSort,
            new FilesystemStreams($this->fstab()),
            $this->optimizer,
            $this->caster,
            $this->putInputIntoRows,
            $entryFactory,
            $this->cacheBatchSize
        );
    }

    public function cache(Cache $cache) : self
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function cacheBatchSize(int $cacheBatchSize) : self
    {
        if ($cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        $this->cacheBatchSize = $cacheBatchSize;

        return $this;
    }

    public function dontPutInputIntoRows() : self
    {
        $this->putInputIntoRows = false;

        return $this;
    }

    public function externalSort(ExternalSort $externalSort) : self
    {
        $this->externalSort = $externalSort;

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

    public function unmount(Filesystem $filesystem) : self
    {
        $this->fstab()->unmount($filesystem);

        return $this;
    }

    private function fstab() : FilesystemTable
    {
        if ($this->fstab === null) {
            $this->fstab = new FilesystemTable(new NativeLocalFilesystem());
        }

        return $this->fstab;
    }
}
