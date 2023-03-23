<?php

declare(strict_types=1);

namespace Flow\ETL;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\NestedPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\VoidPipeline;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Row\References;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Sort;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use Flow\ETL\Transformer\EntryExpressionFilterTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\JoinRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;

final class DataFrame
{
    private FlowContext $context;

    private ?GroupBy $groupBy;

    public function __construct(private Pipeline $pipeline, Config $configuration)
    {
        $this->groupBy = null;
        $this->context = new FlowContext($configuration);
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
        $this->context->config->cache()->clear($id ??= $this->context->config->id());

        foreach ($this->pipeline->process($this->context) as $rows) {
            $this->context->config->cache()->add($id, $rows);
        }

        $this->pipeline = $this->pipeline->cleanCopy();
        $this->context->config->clearLimit();
        $this->pipeline->source(new CacheExtractor($id, $this->context->config->cache()));

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

    public function crossJoin(self $dataFrame, string $prefix = '') : self
    {
        $this->pipeline->add(new CrossJoinRowsTransformer($dataFrame, $prefix));

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
    public function drop(string|Reference ...$entries) : self
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
            $this->context->config->setLimit($limit);
        }

        if ($this->context->partitionEntries()->count()) {
            $rows = (new Rows())->merge(
                ...\iterator_to_array($this->pipeline->process($this->context))
            );

            $fetchedRows = (new Rows());

            foreach ($rows->partitionBy(...$this->context->partitionEntries()->all()) as $partitionedRows) {
                if ($this->context->partitionFilter()->keep(...$partitionedRows->partitions)) {
                    $fetchedRows = $fetchedRows->merge($partitionedRows->rows);
                }
            }

            return $fetchedRows;
        }

        return (new Rows())->merge(
            ...\iterator_to_array($this->pipeline->process($this->context))
        );
    }

    /**
     * @param callable(Row $row) : bool|EntryReference $callback
     */
    public function filter(callable|EntryReference $callback) : self
    {
        if ($callback instanceof EntryReference) {
            $this->pipeline->add(new EntryExpressionFilterTransformer($callback));
        }

        if (\is_callable($callback)) {
            $this->pipeline->add(new FilterRowsTransformer(new Callback($callback)));
        }

        return $this;
    }

    public function filterPartitions(Partition\PartitionFilter $filter) : self
    {
        $this->context->filterPartitions($filter);

        return $this;
    }

    /**
     * @param null|callable(Rows $rows) : void $callback
     */
    public function forEach(callable $callback = null) : void
    {
        $this->run($callback);
    }

    public function groupBy(string|Reference ...$entries) : self
    {
        $this->groupBy = new GroupBy(...$entries);
        $this->pipeline = new GroupByPipeline($this->groupBy, $this->pipeline);

        return $this;
    }

    /**
     * @psalm-param "left"|"left_anti"|"right"|"inner"|Join $type
     */
    public function join(self $dataFrame, Expression $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        /** @psalm-suppress ParadoxicalCondition */
        $transformer = match (\strtolower($type)) {
            Join::left->value => JoinRowsTransformer::left($dataFrame, $on),
            Join::left_anti->value => JoinRowsTransformer::leftAnti($dataFrame, $on),
            Join::right->value => JoinRowsTransformer::right($dataFrame, $on),
            Join::inner->value => JoinRowsTransformer::inner($dataFrame, $on),
            /** @phpstan-ignore-next-line  */
            default => throw new InvalidArgumentException("Unsupported join type: {$type}")
        };

        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @psalm-param "left"|"left_anti"|"right"|"inner"|Join $type
     */
    public function joinEach(DataFrameFactory $factory, Expression $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        /** @psalm-suppress ParadoxicalCondition */
        $transformer = match (\strtolower($type)) {
            Join::left->value => JoinEachRowsTransformer::left($factory, $on),
            Join::left_anti->value => JoinEachRowsTransformer::leftAnti($factory, $on),
            Join::right->value => JoinEachRowsTransformer::right($factory, $on),
            Join::inner->value => JoinEachRowsTransformer::inner($factory, $on),
            /** @phpstan-ignore-next-line  */
            default => throw new InvalidArgumentException("Unsupported join type: {$type}")
        };
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function limit(?int $limit) : self
    {
        if ($limit === null) {
            $this->context->config->clearLimit();
        } else {
            $this->context->config->setLimit($limit);
        }

        return $this;
    }

    public function load(Loader $loader) : self
    {
        $this->pipeline->add($loader);

        return $this;
    }

    /**
     * @param callable(Row $row) : Row $callback
     */
    public function map(callable $callback) : self
    {
        $this->pipeline->add(new CallbackRowTransformer($callback));

        return $this;
    }

    /**
     * SaveMode defines how Flow should behave when writing to a file/files that already exists.
     * For more details please see SaveMode enum.
     *
     * @param SaveMode $mode
     *
     * @return $this
     */
    public function mode(SaveMode $mode) : self
    {
        $this->context->setMode($mode);

        return $this;
    }

    public function onError(ErrorHandler $handler) : self
    {
        $this->context->setErrorHandler($handler);

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

    public function partitionBy(string|Reference $entry, string|Reference ...$entries) : self
    {
        \array_unshift($entries, $entry);

        $this->context->partitionBy(...References::init(...$entries)->all());

        return $this;
    }

    public function pipeline(Pipeline $pipeline) : self
    {
        $this->pipeline = new NestedPipeline($this->pipeline, $pipeline);

        return $this;
    }

    public function printRows(int|null $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : void
    {
        if ($limit === null) {
            $this->context->config->clearLimit();
        } else {
            $this->context->config->setLimit($limit);
        }

        $this->load(To::output($truncate, Output::rows, $formatter));

        $this->run();
    }

    public function printSchema(int|null $limit = 20, Schema\SchemaFormatter $formatter = new Schema\Formatter\ASCIISchemaFormatter()) : void
    {
        if ($limit === null) {
            $this->context->config->clearLimit();
        } else {
            $this->context->config->setLimit($limit);
        }
        $this->load(To::output(false, Output::schema, schemaFormatter: $formatter));

        $this->run();
    }

    public function rename(string $from, string $to) : self
    {
        $this->pipeline->add(Transform::rename($from, $to));

        return $this;
    }

    /**
     * Alias for ETL::transform method.
     */
    public function rows(Transformer|Transformation $transformer) : self
    {
        return $this->transform($transformer);
    }

    /**
     * @param callable(Rows $rows): void|null $callback
     */
    public function run(callable $callback = null) : void
    {
        foreach ($this->pipeline->process($this->context) as $rows) {
            if ($callback !== null) {
                $callback($rows);
            }
        }
    }

    /**
     * Keep only given entries.
     */
    public function select(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new KeepEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @psalm-suppress DeprecatedClass
     */
    public function sortBy(Sort|EntryReference ...$entries) : self
    {
        $this->cache($this->context->config->id());

        $sortBy = [];

        foreach ($entries as $entry) {
            if ($entry instanceof Sort) {
                $sortBy[] = $entry->isAsc() ? ref($entry->name())->asc() : ref($entry->name())->desc();
            } else {
                $sortBy[] = $entry;
            }
        }

        $this->pipeline->source($this->context->config->externalSort()->sortBy(...$sortBy));

        return $this;
    }

    /**
     * When set to true, files are never written under the origin name but instead initial path is turned
     * into a folder in which each process writes to a new file.
     * Otherwise parallel processing would not be possible due to a single file bottleneck.
     * In a single process pipelines there is not much added value from this setting unless
     * there is a chance that the same pipeline execution might overlap.
     */
    public function threadSafe(bool $threadSafe = true) : self
    {
        $this->context->setThreadSafe($threadSafe);

        return $this;
    }

    public function transform(Transformer|Transformation $transformer) : self
    {
        if ($transformer instanceof Transformer) {
            $this->pipeline->add($transformer);

            return $this;
        }

        return $transformer->transform($this);
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

    public function withEntry(string $entryName, EntryReference|Literal $ref) : self
    {
        $this->transform(new EntryExpressionEvalTransformer($entryName, $ref));

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
