<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Join\Condition;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\VoidPipeline;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Sort;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\JoinRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;

final class DataFrame
{
    private Cache $cache;

    private ExternalSort $externalSort;

    private ?GroupBy $groupBy;

    private string $id;

    private ?int $limit;

    private Pipeline $pipeline;

    public function __construct(Extractor $extractor, Config $configuration)
    {
        $this->id = $configuration->id();
        $this->pipeline = $configuration->pipeline();
        $this->pipeline->source($extractor);
        $this->limit = null;
        $this->cache = $configuration->cache();
        $this->externalSort = $configuration->externalSort();
        $this->groupBy = null;
    }

    /**
     * @param Aggregation ...$aggregations
     *
     * @throws InvalidArgumentException
     *
     * @return self
     */
    public function aggregate(Aggregation ...$aggregations) : self
    {
        if ($this->groupBy === null) {
            $this->groupBy = new GroupBy();
        }

        $this->groupBy->aggregate(...$aggregations);

        $this->pipeline = new GroupByPipeline($this->groupBy, $this->pipeline);
        $this->groupBy = null;

        return $this;
    }

    /**
     * Start processing rows up to this moment and put each instance of Rows
     * into previously defined cache.
     * Cache type can be set through ConfigBuilder.
     * By default everything is cached in system tmp dir.
     *
     * @param null|string $id
     *
     * @return self
     */
    public function cache(string $id = null) : self
    {
        $this->cache->clear($id = $id ?? $this->id);

        foreach ($this->pipeline->process($this->limit) as $rows) {
            $this->cache->add($id, $rows);
        }

        $this->pipeline = $this->pipeline->cleanCopy();
        $this->limit = null;
        $this->pipeline->source(new CacheExtractor($id, $this->cache));

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here all transformed Rows are collected and merged together before pushing them forward.
     *
     * @return self
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

    /**
     * Drop given entries.
     *
     * @param string ...$entries
     *
     * @return self
     */
    public function drop(string ...$entries) : self
    {
        $this->pipeline->add(new RemoveEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @param ?int $limit
     *
     * @throws InvalidArgumentException
     *
     * @return Rows
     */
    public function fetch(?int $limit = null) : Rows
    {
        if ($limit !== null) {
            if ($limit <= 0) {
                throw new InvalidArgumentException("Fetch limit can't be lower or equal to 0");
            }

            $this->limit = $limit;
        }

        return (new Rows())->merge(...\iterator_to_array($this->pipeline->process($this->limit)));
    }

    /**
     * @param callable(Row $row) : bool $callback
     * @psalm-param pure-callable(Row $row) : bool $callback
     *
     * @return self
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

    /**
     * @param string ...$entries
     *
     * @return self
     */
    public function groupBy(string ...$entries) : self
    {
        $this->groupBy = new GroupBy(...$entries);
        $this->pipeline = new GroupByPipeline($this->groupBy, $this->pipeline);

        return $this;
    }

    /**
     * @param DataFrame $dataFrame
     * @param Condition $on
     * @param string $type
     * @psalm-param "left"|"right"|"inner" $type
     *
     * @return self
     */
    public function join(self $dataFrame, Condition $on, string $type = 'left') : self
    {
        /** @var Transformer $transformer */
        $transformer = JoinRowsTransformer::$type($dataFrame, $on);
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @param int $limit
     *
     * @throws InvalidArgumentException
     *
     * @return self
     */
    public function limit(int $limit) : self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: {$limit}");
        }

        $this->limit = $limit;

        return $this;
    }

    /**
     * @param Loader $loader
     *
     * @return self
     */
    public function load(Loader $loader) : self
    {
        $this->pipeline->add($loader);

        return $this;
    }

    /**
     * @param callable(Row $row) : Row $callback
     * @psalm-param pure-callable(Row $row) : Row $callback
     *
     * @return self
     */
    public function map(callable $callback) : self
    {
        $this->pipeline->add(new CallbackRowTransformer($callback));

        return $this;
    }

    /**
     * @param ErrorHandler $handler
     *
     * @return self
     */
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
     * @return self
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    /**
     * @param string $from
     * @param string $to
     *
     * @return self
     */
    public function rename(string $from, string $to) : self
    {
        $this->pipeline->add(Transform::rename($from, $to));

        return $this;
    }

    /**
     * Alias for ETL::transform method.
     *
     * @param Transformer $transformer
     *
     * @return self
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

    /**
     * Keep only given entries.
     *
     * @param string ...$entries
     *
     * @return self
     */
    public function select(string ...$entries) : self
    {
        $this->pipeline->add(new KeepEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @param Sort ...$entries
     *
     * @return self
     */
    public function sortBy(Sort ...$entries) : self
    {
        $this->cache($this->id);

        $this->pipeline->source($this->externalSort->sortBy(...$entries));

        return $this;
    }

    /**
     * @param Transformer $transformer
     *
     * @return self
     */
    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @param Schema $schema
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     *
     * @return self
     */
    public function validate(Schema $schema, SchemaValidator $validator = null) : self
    {
        $this->pipeline->add(new SchemaValidationLoader($schema, $validator ?? new Schema\StrictValidator()));

        return $this;
    }

    /**
     * This method is useful mostly in development when
     * you want to pause processing at certain moment without
     * removing code. All operations will get processed up to this point,
     * from here no rows are passed forward.
     *
     * @return self
     */
    public function void() : self
    {
        $this->pipeline = new VoidPipeline($this->pipeline);

        return $this;
    }

    /**
     * Alias for ETL::load function.
     *
     * @param Loader $loader
     *
     * @return self
     */
    public function write(Loader $loader) : self
    {
        return $this->load($loader);
    }
}
