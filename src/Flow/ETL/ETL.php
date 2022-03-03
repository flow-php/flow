<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Sort;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\FilterRowsTransformer;

final class ETL
{
    private Cache $cache;

    private ExternalSort $externalSort;

    private string $id;

    private ?int $limit;

    private Pipeline $pipeline;

    private function __construct(Extractor $extractor, Config $configuration)
    {
        $this->id = $configuration->id();
        $this->pipeline = $configuration->pipeline();
        $this->pipeline->source($extractor);
        $this->limit = null;
        $this->cache = $configuration->cache();
        $this->externalSort = $configuration->externalSort();
    }

    public static function extract(Extractor $extractor, Config $configuration = null) : self
    {
        return new self(
            $extractor,
            $configuration ?? Config::default()
        );
    }

    public static function process(Rows $rows, Config $configuration = null) : self
    {
        return new self(
            new ProcessExtractor($rows),
            $configuration ?? Config::default()
        );
    }

    /**
     * Alias for ETL::extract function.
     *
     * @param Extractor $extractor
     * @param null|Config $configuration
     *
     * @return static
     */
    public static function read(Extractor $extractor, Config $configuration = null) : self
    {
        return self::extract($extractor, $configuration);
    }

    public function cache(string $id = null) : self
    {
        $this->cache->clear($id = $id ?? $this->id);

        foreach ($this->pipeline->process($this->limit) as $rows) {
            $this->cache->add($id, $rows);
        }

        $this->pipeline = $this->pipeline->clean();
        $this->limit = null;
        $this->pipeline->source(new CacheExtractor($id, $this->cache));

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

    public function fetch(int $limit = 0) : Rows
    {
        if ($limit < 0) {
            throw new InvalidArgumentException("Fetch limit can't be lower than 0");
        }

        if ($limit !== 0) {
            $this->limit = $limit;
        }

        return (new Rows())->merge(...\iterator_to_array($this->pipeline->process($this->limit)));
    }

    /**
     * @param callable(Row $row) : bool $callback
     * @psalm-param pure-callable(Row $row) : bool $callback
     *
     * @return $this
     */
    public function filter(callable $callback) : self
    {
        $this->pipeline->add(new FilterRowsTransformer(new Callback($callback)));

        return $this;
    }

    /**
     * @param null|callable(Rows $rows) : void $callback
     */
    public function forEach(callable $callback = null) : void
    {
        $this->run($callback);
    }

    public function limit(int $limit) : self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: {$limit}");
        }

        $this->limit = $limit;

        return $this;
    }

    public function load(Loader $loader) : self
    {
        $this->pipeline->add($loader);

        return $this;
    }

    /**
     * @param callable(Row $row) : Row $callback
     * @psalm-param pure-callable(Row $row) : Row $callback
     *
     * @return $this
     */
    public function map(callable $callback) : self
    {
        $this->pipeline->add(new CallbackRowTransformer($callback));

        return $this;
    }

    public function onError(ErrorHandler $handler) : self
    {
        $this->pipeline->onError($handler);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here each transformed Row is divided and pushed forward to following pipeline elements.
     *
     * @param int $chunks
     *
     * @throws InvalidArgumentException
     *
     * @return ETL
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    /**
     * Alias for ETL::transform method.
     *
     * @param Transformer $transformer
     *
     * @return $this
     */
    public function rows(Transformer $transformer) : self
    {
        return $this->transform($transformer);
    }

    /**
     * @param callable(Rows $rows): void|null $callback
     */
    public function run(callable $callback = null) : void
    {
        foreach ($this->pipeline->process($this->limit) as $rows) {
            if ($callback !== null) {
                $callback($rows);
            }
        }
    }

    public function sortBy(Sort ...$entries) : self
    {
        $this->cache($this->id);

        $this->pipeline->source($this->externalSort->sortBy(...$entries));

        return $this;
    }

    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->add($transformer);

        return $this;
    }

    public function validate(Schema $schema) : self
    {
        $this->pipeline->add(new SchemaValidationLoader($schema));

        return $this;
    }

    /**
     * Alias for ETL::load function.
     *
     * @param Loader $loader
     *
     * @return $this
     */
    public function write(Loader $loader) : self
    {
        return $this->load($loader);
    }
}
