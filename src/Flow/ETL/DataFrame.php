<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Join\Condition;
use Flow\ETL\Join\Join;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\NestedPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\VoidPipeline;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Sort;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\JoinRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;

final class DataFrame
{
    private ?GroupBy $groupBy;

    public function __construct(private Pipeline $pipeline, private readonly Config $configuration)
    {
        $this->groupBy = null;
    }

    /**
     * @throws InvalidArgumentException
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
     */
    public function cache(string $id = null) : self
    {
        $this->configuration->cache()->clear($id ??= $this->configuration->id());

        foreach ($this->pipeline->process($this->configuration) as $rows) {
            $this->configuration->cache()->add($id, $rows);
        }

        $this->pipeline = $this->pipeline->cleanCopy();
        $this->configuration->clearLimit();
        $this->pipeline->source(new CacheExtractor($id, $this->configuration->cache()));

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
     * @param bool|int $truncate false or if set to 0 columns are not truncated, otherwise default truncate to 20 characters
     * @param Formatter $formatter
     *
     * @throws InvalidArgumentException
     */
    public function display(int $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : string
    {
        return $formatter->format($this->fetch($limit), $truncate);
    }

    /**
     * Drop given entries.
     */
    public function drop(string ...$entries) : self
    {
        $this->pipeline->add(new RemoveEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function fetch(?int $limit = null) : Rows
    {
        if ($limit !== null) {
            $this->configuration->setLimit($limit);
        }

        return (new Rows())->merge(
            ...\iterator_to_array($this->pipeline->process($this->configuration))
        );
    }

    /**
     * @param callable(Row $row) : bool $callback
     * @psalm-param pure-callable(Row $row) : bool $callback
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

    public function groupBy(string ...$entries) : self
    {
        $this->groupBy = new GroupBy(...$entries);
        $this->pipeline = new GroupByPipeline($this->groupBy, $this->pipeline);

        return $this;
    }

    /**
     * @psalm-param "left"|"right"|"inner"|Join $type
     */
    public function join(self $dataFrame, Condition $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        /** @var Transformer $transformer */
        $transformer = JoinRowsTransformer::$type($dataFrame, $on);
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @psalm-param "left"|"right"|"inner"|Join $type
     */
    public function joinEach(DataFrameFactory $factory, Condition $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        /** @var Transformer $transformer */
        $transformer = JoinEachRowsTransformer::$type($factory, $on);
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function limit(int $limit) : self
    {
        $this->configuration->setLimit($limit);

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
     */
    public function map(callable $callback) : self
    {
        $this->pipeline->add(new CallbackRowTransformer($callback));

        return $this;
    }

    public function onError(ErrorHandler $handler) : self
    {
        $this->configuration->setErrorHandler($handler);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here each transformed Row is divided and pushed forward to following pipeline elements.
     *
     * @throws InvalidArgumentException
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    public function pipeline(Pipeline $pipeline) : self
    {
        $this->pipeline = new NestedPipeline($this->pipeline, $pipeline);

        return $this;
    }

    public function rename(string $from, string $to) : self
    {
        $this->pipeline->add(Transform::rename($from, $to));

        return $this;
    }

    /**
     * Alias for ETL::transform method.
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
        foreach ($this->pipeline->process($this->configuration) as $rows) {
            if ($callback !== null) {
                $callback($rows);
            }
        }
    }

    /**
     * Keep only given entries.
     */
    public function select(string ...$entries) : self
    {
        $this->pipeline->add(new KeepEntriesTransformer(...$entries));

        return $this;
    }

    public function sortBy(Sort ...$entries) : self
    {
        $this->cache($this->configuration->id());

        $this->pipeline->source($this->configuration->externalSort()->sortBy(...$entries));

        return $this;
    }

    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
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
     */
    public function void() : self
    {
        $this->pipeline = new VoidPipeline($this->pipeline);

        return $this;
    }

    /**
     * Alias for ETL::load function.
     */
    public function write(Loader $loader) : self
    {
        return $this->load($loader);
    }
}
