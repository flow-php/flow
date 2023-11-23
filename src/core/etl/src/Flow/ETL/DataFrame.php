<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Pipeline\BatchingPipeline;
use Flow\ETL\Pipeline\CachingPipeline;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\PartitioningPipeline;
use Flow\ETL\Pipeline\VoidPipeline;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\Schema;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\JoinRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Flow\ETL\Transformer\UntilTransformer;
use Flow\ETL\Transformer\WindowFunctionTransformer;

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
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function aggregate(AggregatingFunction ...$aggregations) : self
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
     * @lazy
     * When set to true, files are never written under the origin name but instead initial path is turned
     * into a folder in which each process writes to a new file.
     * This is also mandatory for SaveMode::Append
     */
    public function appendSafe(bool $appendSafe = true) : self
    {
        $this->context->setAppendSafe($appendSafe);

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches of given size.
     * For example, when Extractor is yielding one row at time, this method will merge them into batches of given size
     * before passing them to the next pipeline element.
     * Similarly when Extractor is yielding batches of rows, this method will split them into smaller batches of given size.
     *
     * In order to merge all Rows into a single batch use DataFrame::collect() method.
     *
     * @param int<1, max> $size
     *
     * @lazy
     */
    public function batchSize(int $size) : self
    {
        $this->pipeline = new BatchingPipeline($this->pipeline, $size);

        return $this;
    }

    /**
     * Start processing rows up to this moment and put each instance of Rows
     * into previously defined cache.
     * Cache type can be set through ConfigBuilder.
     * By default everything is cached in system tmp dir.
     *
     * @lazy
     *
     * @param null|string $id
     */
    public function cache(?string $id = null) : self
    {
        $this->pipeline = new CachingPipeline($this->pipeline, $id);

        return $this;
    }

    /**
     * Before transforming rows, collect them and merge into single Rows instance.
     * This might lead to memory issues when processing large amount of rows, use with caution.
     *
     * @lazy
     */
    public function collect() : self
    {
        $this->pipeline = new CollectingPipeline($this->pipeline);

        return $this;
    }

    /**
     * @trigger
     * Return total count of rows processed by this pipeline.
     */
    public function count() : int
    {
        $clone = clone $this;

        $total = 0;

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            $total += $rows->count();
        }

        return $total;
    }

    /**
     * @lazy
     */
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
     * @trigger
     *
     * @throws InvalidArgumentException
     */
    public function display(int $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : string
    {
        return $formatter->format($this->fetch($limit), $truncate);
    }

    /**
     * Drop given entries.
     *
     * @lazy
     */
    public function drop(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new RemoveEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @param Reference|string ...$entries
     *
     * @lazy
     *
     * @return $this
     */
    public function dropDuplicates(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new DropDuplicatesTransformer(...$entries));

        return $this;
    }

    /**
     * Be aware that fetch is not memory safe and will load all rows into memory.
     * If you want to safely iterate over Rows use oe of the following methods:.
     *
     * DataFrame::get() : \Generator
     * DataFrame::getAsArray() : \Generator
     * DataFrame::getEach() : \Generator
     * DataFrame::getEachAsArray() : \Generator
     *
     * @trigger
     *
     * @throws InvalidArgumentException
     */
    public function fetch(?int $limit = null) : Rows
    {
        $clone = clone $this;
        $clone->collect();

        if ($limit !== null) {
            $clone->limit($limit);
        }

        if ($clone->context->partitionEntries()->count()) {
            $rows = (new Rows())->merge(
                ...\iterator_to_array($clone->pipeline->process($clone->context))
            );

            $fetchedRows = (new Rows());

            foreach ($rows->partitionBy(...$clone->context->partitionEntries()->all()) as $partitionedRows) {
                if ($clone->context->partitionFilter()->keep(...$partitionedRows->partitions())) {
                    $fetchedRows = $fetchedRows->merge($partitionedRows);
                }
            }

            return $fetchedRows;
        }

        $rows = \iterator_to_array($clone->pipeline->process($clone->context));

        if (!\count($rows)) {
            return new Rows();
        }

        return $rows[0];
    }

    /**
     * @lazy
     */
    public function filter(ScalarFunction $function) : self
    {
        $this->pipeline->add(new ScalarFunctionFilterTransformer($function));

        return $this;
    }

    /**
     * @lazy
     */
    public function filterPartitions(Partition\PartitionFilter $filter) : self
    {
        $this->context->filterPartitions($filter);

        return $this;
    }

    /**
     * @trigger
     *
     * @param null|callable(Rows $rows) : void $callback
     */
    public function forEach(?callable $callback = null) : void
    {
        $clone = clone $this;
        $clone->run($callback);
    }

    /**
     * Yields each row as an instance of Rows.
     *
     * @trigger
     *
     * @return \Generator<Rows>
     */
    public function get() : \Generator
    {
        $clone = clone $this;

        return $clone->pipeline->process($clone->context);
    }

    /**
     * Yields each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array<array>>
     */
    public function getAsArray() : \Generator
    {
        $clone = clone $this;

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            yield $rows->toArray();
        }
    }

    /**
     * Yield each row as an instance of Row.
     *
     * @trigger
     *
     * @return \Generator<Row>
     */
    public function getEach() : \Generator
    {
        $clone = clone $this;

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            foreach ($rows as $row) {
                yield $row;
            }
        }
    }

    /**
     * Yield each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array>
     */
    public function getEachAsArray() : \Generator
    {
        $clone = clone $this;

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            foreach ($rows as $row) {
                yield $row->toArray();
            }
        }
    }

    /**
     * @lazy
     */
    public function groupBy(string|Reference ...$entries) : self
    {
        $this->groupBy = new GroupBy(...$entries);
        $this->pipeline = new GroupByPipeline($this->groupBy, $this->pipeline);

        return $this;
    }

    /**
     * @lazy
     *
     * @psalm-param string|Join $type
     */
    public function join(self $dataFrame, Expression $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        $transformer = match ($type) {
            Join::left->value => JoinRowsTransformer::left($dataFrame, $on),
            Join::left_anti->value => JoinRowsTransformer::leftAnti($dataFrame, $on),
            Join::right->value => JoinRowsTransformer::right($dataFrame, $on),
            Join::inner->value => JoinRowsTransformer::inner($dataFrame, $on),
            default => throw new InvalidArgumentException('Unsupported join type')
        };

        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @lazy
     *
     * @psalm-param string|Join $type
     */
    public function joinEach(DataFrameFactory $factory, Expression $on, string|Join $type = Join::left) : self
    {
        if ($type instanceof Join) {
            $type = $type->name;
        }

        $transformer = match ($type) {
            Join::left->value => JoinEachRowsTransformer::left($factory, $on),
            Join::left_anti->value => JoinEachRowsTransformer::leftAnti($factory, $on),
            Join::right->value => JoinEachRowsTransformer::right($factory, $on),
            Join::inner->value => JoinEachRowsTransformer::inner($factory, $on),
            default => throw new InvalidArgumentException('Unsupported join type')
        };
        $this->pipeline->add($transformer);

        return $this;
    }

    /**
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function limit(int $limit) : self
    {
        $this->pipeline = $this->context->config->optimizer()->optimize(new LimitTransformer($limit), $this->pipeline);

        return $this;
    }

    /**
     * @lazy
     */
    public function load(Loader $loader) : self
    {
        $this->pipeline = $this->context->config->optimizer()->optimize($loader, $this->pipeline);

        return $this;
    }

    /**
     * @lazy
     *
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
     * @lazy
     *
     * @return $this
     */
    public function mode(SaveMode $mode) : self
    {
        $this->context->streams()->setSaveMode($mode);

        if ($mode === SaveMode::Append) {
            $this->appendSafe();
        }

        return $this;
    }

    /**
     * @lazy
     */
    public function onError(ErrorHandler $handler) : self
    {
        $this->context->setErrorHandler($handler);

        return $this;
    }

    /**
     * @deprecated - use DataFrame::batchSize() instead
     *
     * @psalm-suppress DeprecatedClass
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here each transformed Row is divided and pushed forward to following pipeline elements.
     *
     * @lazy
     *
     * @param int<1, max> $chunks
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    /**
     * @lazy
     */
    public function partitionBy(string|Reference $entry, string|Reference ...$entries) : self
    {
        \array_unshift($entries, $entry);

        $this->context->partitionBy(...References::init(...$entries)->all());
        $this->pipeline = new PartitioningPipeline($this->pipeline);

        return $this;
    }

    public function pivot(Reference $ref) : self
    {
        if ($this->groupBy === null) {
            throw new RuntimeException('Pivot can be used only with groupBy');
        }

        $this->groupBy->pivot($ref);

        return $this;
    }

    /**
     * @trigger
     */
    public function printRows(int|null $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : void
    {
        $clone = clone $this;

        if ($limit !== null) {
            $clone->limit($limit);
        }

        $clone->load(To::output($truncate, Output::rows, $formatter));

        $clone->run();
    }

    /**
     * @trigger
     */
    public function printSchema(int|null $limit = 20, Schema\SchemaFormatter $formatter = new Schema\Formatter\ASCIISchemaFormatter()) : void
    {
        $clone = clone $this;

        if ($limit !== null) {
            $clone->limit($limit);
        }
        $clone->load(To::output(false, Output::schema, schemaFormatter: $formatter));

        $clone->run();
    }

    /**
     * @lazy
     */
    public function rename(string $from, string $to) : self
    {
        $this->pipeline->add(Transform::rename($from, $to));

        return $this;
    }

    /**
     * @lazy
     * Iterate over all entry names and replace given search string with replace string.
     */
    public function renameAll(string $search, string $replace) : self
    {
        $this->pipeline->add(Transform::rename_str_replace_all($search, $replace));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllLowerCase() : self
    {
        $this->pipeline->add(Transform::rename_all_case(lower: true));

        return $this;
    }

    /**
     * @lazy
     * Rename all entries to given style.
     * Please look into \Flow\ETL\Transformer\StyleConverter\StringStyles class for all available styles.
     */
    public function renameAllStyle(StringStyles|string $style) : self
    {
        $this->pipeline->add(Transform::convert_name($style));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCase() : self
    {
        $this->pipeline->add(Transform::rename_all_case(upper: true));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCaseFirst() : self
    {
        $this->pipeline->add(Transform::rename_all_case(ucfirst: true));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCaseWord() : self
    {
        $this->pipeline->add(Transform::rename_all_case(ucwords: true));

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::transform method.
     */
    public function rows(Transformer|Transformation $transformer) : self
    {
        return $this->transform($transformer);
    }

    /**
     * @trigger
     *
     * @param null|callable(Rows $rows): void $callback
     */
    public function run(?callable $callback = null) : void
    {
        $clone = clone $this;

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            if ($callback !== null) {
                $callback($rows);
            }
        }
    }

    /**
     * @lazy
     * Keep only given entries.
     */
    public function select(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new KeepEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @lazy
     */
    public function sortBy(Reference ...$entries) : self
    {
        $this
            ->cache($this->context->config->id())
            ->run();

        $this->pipeline = $this->pipeline->cleanCopy();
        $this->pipeline->setSource($this->context->config->externalSort()->sortBy(...$entries));

        return $this;
    }

    /**
     * @deprecated please use DataFrame::appendSafe() instead
     *
     * @lazy
     */
    public function threadSafe(bool $appendSafe = true) : self
    {
        $this->appendSafe($appendSafe);

        return $this;
    }

    /**
     * @lazy
     */
    public function transform(Transformer|Transformation $transformer) : self
    {
        if ($transformer instanceof Transformer) {
            $this->pipeline->add($transformer);

            return $this;
        }

        return $transformer->transform($this);
    }

    /**
     * The difference between filter and until is that filter will keep filtering rows until extractors finish yielding rows.
     * Until will send a STOP signal to the Extractor when the condition is not met.
     *
     * @lazy
     */
    public function until(ScalarFunction $function) : self
    {
        $this->pipeline->add(new UntilTransformer($function));

        return $this;
    }

    /**
     * @lazy
     *
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     */
    public function validate(Schema $schema, ?SchemaValidator $validator = null) : self
    {
        $this->pipeline->add(new SchemaValidationLoader($schema, $validator ?? new Schema\StrictValidator()));

        return $this;
    }

    /**
     * @lazy
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
     * @lazy
     *
     * @param array<string, \Flow\ETL\Function\ScalarFunction> $refs
     */
    public function withEntries(array $refs) : self
    {
        foreach ($refs as $entryName => $ref) {
            $this->withEntry($entryName, $ref);
        }

        return $this;
    }

    /**
     * @lazy
     */
    public function withEntry(string $entryName, Function\ScalarFunction|WindowFunction $ref) : self
    {
        if ($ref instanceof WindowFunction) {
            if (\count($ref->window()->partitions())) {
                $this->context->partitionBy(...$ref->window()->partitions());
                $this->pipeline = new PartitioningPipeline($this->pipeline, $ref->window()->order());
            } else {
                $this->collect();

                if (\count($ref->window()->order())) {
                    $this->sortBy(...$ref->window()->order());
                }
            }

            $this->pipeline->add(new WindowFunctionTransformer($entryName, $ref));
        } else {
            $this->transform(new ScalarFunctionTransformer($entryName, $ref));
        }

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::load function.
     */
    public function write(Loader $loader) : self
    {
        return $this->load($loader);
    }
}
