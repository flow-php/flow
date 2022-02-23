<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\ExternalSort\CacheExternalSort;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Row\Sort;

final class ETL
{
    private string $uniqueId;

    private ?int $limit;

    private Extractor $extractor;

    private Pipeline $pipeline;

    private Cache $cache;

    private ExternalSort $externalSort;

    private function __construct(Extractor $extractor, Pipeline $pipeline)
    {
        $this->uniqueId = \uniqid('flow_php_');

        $this->extractor = $extractor;
        $this->pipeline = $pipeline;
        $this->limit = null;
        $this->cache = new LocalFilesystemCache();
        $this->externalSort = new CacheExternalSort($this->uniqueId, $this->cache);
    }

    public static function process(Rows $rows, Pipeline $pipeline = null) : self
    {
        return new self(
            new ProcessExtractor($rows),
            $pipeline ?? new SynchronousPipeline(),
        );
    }

    public static function extract(Extractor $extractor, Pipeline $pipeline = null) : self
    {
        return new self(
            $extractor,
            $pipeline ?? new SynchronousPipeline()
        );
    }

    public function onError(ErrorHandler $handler) : self
    {
        $this->pipeline->onError($handler);

        return $this;
    }

    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->add($transformer);

        return $this;
    }

    public function load(Loader $loader) : self
    {
        $this->pipeline->add($loader);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here all transformed Rows are collected and merged together before pushing them forward.
     */
    public function collect() : self
    {
        $this->pipeline = new CollectingPipeline($this->pipeline);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here each transformed Row is divided and pushed forward to following pipeline elements.
     *
     * @throws Exception\InvalidArgumentException
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    public function fetch(int $limit = 0) : Rows
    {
        if ($limit < 0) {
            throw new InvalidArgumentException("Fetch limit can't be lower than 0");
        }

        if ($limit !== 0) {
            $this->limit = $limit;
        }

        $rows = new Rows();
        $this->pipeline->process($this->extractor->extract(), $this->limit, function (Rows $nextRows) use (&$rows) : void {
            /**
             * @psalm-suppress MixedMethodCall
             * @psalm-suppress MixedAssignment
             */
            $rows = $rows->merge($nextRows);
        });

        /** @var Rows $rows */
        return $rows;
    }

    public function limit(int $limit) : self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: {$limit}");
        }

        $this->limit = $limit;

        return $this;
    }

    /**
     * @param int $limit maximum numbers of rows to display
     * @param int $truncate if set to 0 columns are not truncated
     * @param null|Formatter $formatter
     *
     * @throws InvalidArgumentException
     *
     * @return string
     */
    public function display(int $limit = 20, int $truncate = 20, Formatter $formatter = null) : string
    {
        $formatter = $formatter ?? new AsciiTableFormatter();

        return $formatter->format($this->fetch($limit), $truncate);
    }

    public function run() : void
    {
        $this->pipeline->process($this->extractor->extract(), $this->limit);
    }

    public function cache(string $id = null) : self
    {
        $this->cache->clear($id = $id ?? $this->uniqueId);

        $this->pipeline->process($this->extractor->extract(), $this->limit, function (Rows $rows) use ($id) : void {
            $this->cache->add($id, $rows);
        });

        $this->pipeline = $this->pipeline->clean();
        $this->limit = null;
        $this->extractor = new CacheExtractor($id, $this->cache);

        return $this;
    }

    public function sortBy(Sort ...$entries) : self
    {
        $this->cache($this->uniqueId);

        $this->extractor = $this->externalSort->sortBy(...$entries);

        return $this;
    }
}
