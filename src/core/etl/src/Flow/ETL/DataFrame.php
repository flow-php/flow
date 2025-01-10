<?php

declare(strict_types=1);

namespace Flow\ETL;

use function Flow\ETL\DSL\{refs, to_output};
use Flow\ETL\DataFrame\GroupedDataFrame;
use Flow\ETL\Dataset\{Report, Statistics};
use Flow\ETL\Exception\{InvalidArgumentException, InvalidFileFormatException, RuntimeException};
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Filesystem\{SaveMode, ScalarFunctionFilter};
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Function\{AggregatingFunction, ScalarFunction, StyleConverter\StringStyles, WindowFunction};
use Flow\ETL\Join\{Expression, Join};
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\PHP\Type\{AutoCaster};
use Flow\ETL\Pipeline\{BatchingPipeline,
    CachingPipeline,
    CollectingPipeline,
    GroupByPipeline,
    HashJoinPipeline,
    LinkedPipeline,
    PartitioningPipeline,
    SortingPipeline,
    VoidPipeline};
use Flow\ETL\Row\{Reference, References, Schema, Schema\Definition};
use Flow\ETL\Transformer\{
    AutoCastTransformer,
    CallbackRowTransformer,
    CrossJoinRowsTransformer,
    DropDuplicatesTransformer,
    DropEntriesTransformer,
    DropPartitionsTransformer,
    EntryNameStyleConverterTransformer,
    JoinEachRowsTransformer,
    LimitTransformer,
    OrderEntriesTransformer,
    OrderEntries\Comparator,
    OrderEntries\TypeComparator,
    RenameAllCaseTransformer,
    RenameEntryTransformer,
    RenameStrReplaceAllEntriesTransformer,
    ScalarFunctionFilterTransformer,
    ScalarFunctionTransformer,
    SelectEntriesTransformer,
    UntilTransformer,
    WindowFunctionTransformer
};
use Flow\Filesystem\Path\Filter;
use Flow\RDSL\AccessControl\{AllowAll, AllowList, DenyAll};
use Flow\RDSL\Attribute\DSLMethod;
use Flow\RDSL\{Builder, DSLNamespace, Executor, Finder};

final class DataFrame
{
    private FlowContext $context;

    public function __construct(private Pipeline $pipeline, Config|FlowContext $context)
    {
        $this->context = $context instanceof FlowContext ? $context : new FlowContext($context);
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $definition) : self
    {
        $namespaces = [
            DSLNamespace::global(new DenyAll()),
            new DSLNamespace('\Flow\ETL\DSL\Adapter\Avro', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\ChartJS', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\CSV', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\Doctrine', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\Elasticsearch', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\GoogleSheet', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\JSON', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\Meilisearch', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\Parquet', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\Text', new AllowAll()),
            new DSLNamespace('\Flow\ETL\Adapter\XML', new AllowAll()),
            new DSLNamespace('\Flow\ETL\DSL', new AllowAll()),
        ];

        try {
            $builder = new Builder(
                new Finder(
                    $namespaces,
                    entryPointACL: new AllowList(['data_frame', 'df']),
                    methodACL: new AllowAll()
                )
            );

            $results = (new Executor())->execute($builder->parse($definition));

            if (\count($results) !== 1) {
                throw new InvalidArgumentException('Invalid JSON, please make sure that there is only one data_frame function');
            }

            if (!$results[0] instanceof self) {
                throw new InvalidArgumentException('Invalid JSON, expected DataFrame instance but got ' . $results[0]::class);
            }

            return $results[0];
        } catch (\Flow\RDSL\Exception\InvalidArgumentException $e) {
            throw new InvalidArgumentException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromJson(string $json) : self
    {
        try {
            return self::fromArray(\json_decode($json, true, 512, JSON_THROW_ON_ERROR));
        } catch (\JsonException $exception) {
            throw new InvalidFileFormatException('json', 'unknown', $exception);
        }
    }

    /**
     * @lazy
     */
    public function aggregate(AggregatingFunction ...$aggregations) : self
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(...$aggregations);

        $this->pipeline = new LinkedPipeline(new GroupByPipeline($groupBy, $this->pipeline));

        return $this;
    }

    public function autoCast() : self
    {
        $this->pipeline->add(new AutoCastTransformer(new AutoCaster($this->context->config->caster())));

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches of given size.
     * For example, when Extractor is yielding one row at time, this method will merge them into batches of given size
     * before passing them to the next pipeline element.
     * Similarly when Extractor is yielding batches of rows, this method will split them into smaller batches of given size.
     *
     * In order to merge all Rows into a single batch use DataFrame::collect() method or set size to -1 or 0.
     *
     * @param int<-1, max> $size
     *
     * @lazy
     */
    public function batchSize(int $size) : self
    {
        if ($size === -1 || $size === 0) {
            return $this->collect();
        }

        $this->pipeline = new LinkedPipeline(new BatchingPipeline($this->pipeline, $size));

        return $this;
    }

    /**
     * Start processing rows up to this moment and put each instance of Rows
     * into previously defined cache.
     * Cache type can be set through ConfigBuilder.
     * By default everything is cached in system tmp dir.
     *
     * Important: cache batch size might significantly improve performance when processing large amount of rows.
     * Larger batch size will increase memory consumption but will reduce number of IO operations.
     * When not set, the batch size is taken from the last DataFrame::batchSize() call.
     *
     * @lazy
     *
     * @param null|string $id
     *
     * @throws InvalidArgumentException
     */
    public function cache(?string $id = null, ?int $cacheBatchSize = null) : self
    {
        if ($cacheBatchSize !== null && $cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        if ($cacheBatchSize) {
            $this->pipeline = new LinkedPipeline(new CachingPipeline(new BatchingPipeline($this->pipeline, $cacheBatchSize), $id));
        } else {
            $this->pipeline = new LinkedPipeline(new CachingPipeline($this->pipeline, $id));
        }

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
        $this->pipeline = new LinkedPipeline(new CollectingPipeline($this->pipeline));

        return $this;
    }

    /**
     * This method allows to collect references to all entries used in this pipeline.
     *
     * ```php
     * (new Flow())
     *   ->read(From::chain())
     *   ->collectRefs($refs = refs())
     *   ->run();
     * ```
     *
     * @lazy
     */
    #[DSLMethod(exclude: true)]
    public function collectRefs(References $references) : self
    {
        $this->transform(new CallbackRowTransformer(function (Row $row) use ($references) : Row {
            foreach ($row->entries()->all() as $entry) {
                $references->add($entry->ref());
            }

            return $row;
        }));

        return $this;
    }

    /**
     * @trigger
     * Return total count of rows processed by this pipeline.
     */
    #[DSLMethod(exclude: true)]
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
    #[DSLMethod(exclude: true)]
    public function display(int $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : string
    {
        $clone = clone $this;
        $clone->limit($limit);

        $output = '';

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            $output .= $formatter->format($rows, $truncate);
        }

        return $output;
    }

    /**
     * Drop given entries.
     *
     * @lazy
     */
    public function drop(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new DropEntriesTransformer(...$entries));

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
     * Drop all partitions from Rows, additionally when $dropPartitionColumns is set to true, partition columns are also removed.
     *
     * @lazy
     */
    public function dropPartitions(bool $dropPartitionColumns = false) : self
    {
        $this->pipeline->add(new DropPartitionsTransformer($dropPartitionColumns));

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
    #[DSLMethod(exclude: true)]
    public function fetch(?int $limit = null) : Rows
    {
        $clone = clone $this;

        if ($limit !== null) {
            $clone->limit($limit);
        }

        $rows = new Rows();

        foreach ($clone->pipeline->process($clone->context) as $nextRows) {
            $rows = $rows->merge($nextRows);
        }

        return $rows;
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
     *
     * @throws RuntimeException
     */
    public function filterPartitions(Filter|ScalarFunction $filter) : self
    {
        $extractor = $this->pipeline->source();

        if (!$extractor instanceof FileExtractor) {
            throw new RuntimeException('filterPartitions can be used only with extractors that implement FileExtractor interface');
        }

        if ($filter instanceof Filter) {
            $extractor->withPathFilter($filter);

            return $this;
        }

        $extractor->withPathFilter(
            new ScalarFunctionFilter(
                $filter,
                $this->context->entryFactory(),
                new AutoCaster($this->context->config->caster())
            )
        );

        return $this;
    }

    /**
     * @lazy
     *
     * @param array<ScalarFunction> $functions
     */
    public function filters(array $functions) : self
    {
        foreach ($functions as $function) {
            $this->filter($function);
        }

        return $this;
    }

    /**
     * @trigger
     *
     * @param null|callable(Rows $rows) : void $callback
     */
    #[DSLMethod(exclude: true)]
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
    #[DSLMethod(exclude: true)]
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
    #[DSLMethod(exclude: true)]
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
    #[DSLMethod(exclude: true)]
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
    #[DSLMethod(exclude: true)]
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
    public function groupBy(string|Reference ...$entries) : GroupedDataFrame
    {
        return new GroupedDataFrame($this, new GroupBy(...$entries));
    }

    /**
     * @lazy
     */
    public function join(self $dataFrame, Expression $on, string|Join $type = Join::left) : self
    {
        if (\is_string($type)) {
            $type = Join::from($type);
        }

        $this->pipeline = new LinkedPipeline(new HashJoinPipeline($this->pipeline, $dataFrame, $on, $type));

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
            default => throw new InvalidArgumentException('Unsupported join type'),
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
    #[DSLMethod(exclude: true)]
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
     * @lazy
     */
    public function partitionBy(string|Reference $entry, string|Reference ...$entries) : self
    {
        \array_unshift($entries, $entry);

        $this->pipeline = new LinkedPipeline(new PartitioningPipeline($this->pipeline, References::init(...$entries)->all()));

        return $this;
    }

    public function pivot(Reference $ref) : self
    {
        if (!$this->pipeline instanceof GroupByPipeline) {
            throw new RuntimeException('Pivot can be used only after groupBy');
        }

        $this->pipeline->groupBy->pivot($ref);

        return $this;
    }

    /**
     * @trigger
     */
    #[DSLMethod(exclude: true)]
    public function printRows(?int $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : void
    {
        $clone = clone $this;

        if ($limit !== null) {
            $clone->limit($limit);
        }

        $clone->load(to_output($truncate, Output::rows, $formatter));

        $clone->run();
    }

    /**
     * @trigger
     */
    #[DSLMethod(exclude: true)]
    public function printSchema(?int $limit = 20, Schema\SchemaFormatter $formatter = new Schema\Formatter\ASCIISchemaFormatter()) : void
    {
        $clone = clone $this;

        if ($limit !== null) {
            $clone->limit($limit);
        }
        $clone->load(to_output(false, Output::schema, schemaFormatter: $formatter));

        $clone->run();
    }

    /**
     * @lazy
     */
    public function rename(string $from, string $to) : self
    {
        $this->pipeline->add(new RenameEntryTransformer($from, $to));

        return $this;
    }

    /**
     * @lazy
     * Iterate over all entry names and replace given search string with replace string.
     */
    public function renameAll(string $search, string $replace) : self
    {
        $this->pipeline->add(new RenameStrReplaceAllEntriesTransformer($search, $replace));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllLowerCase() : self
    {
        $this->pipeline->add(new RenameAllCaseTransformer(lower: true));

        return $this;
    }

    /**
     * @lazy
     * Rename all entries to given style.
     * Please look into \Flow\ETL\Function\StyleConverter\StringStyles class for all available styles.
     */
    public function renameAllStyle(StringStyles|string $style) : self
    {
        $this->pipeline->add(new EntryNameStyleConverterTransformer(\is_string($style) ? StringStyles::fromString($style) : $style));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCase() : self
    {
        $this->pipeline->add(new RenameAllCaseTransformer(upper: true));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCaseFirst() : self
    {
        $this->pipeline->add(new RenameAllCaseTransformer(ucfirst: true));

        return $this;
    }

    /**
     * @lazy
     */
    public function renameAllUpperCaseWord() : self
    {
        $this->pipeline->add(new RenameAllCaseTransformer(ucwords: true));

        return $this;
    }

    public function reorderEntries(Comparator $comparator = new TypeComparator()) : self
    {
        $this->pipeline->add(new OrderEntriesTransformer($comparator));

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
     * @param bool $analyze - when set to true, run will return Report
     */
    #[DSLMethod(exclude: true)]
    public function run(?callable $callback = null, bool $analyze = false) : ?Report
    {
        $clone = clone $this;

        $totalRows = 0;
        $schema = new Schema();

        foreach ($clone->pipeline->process($clone->context) as $rows) {
            if ($callback !== null) {
                $callback($rows);
            }

            if ($analyze) {
                $schema = $schema->merge($rows->schema());
                $totalRows += $rows->count();
            }
        }

        if ($analyze) {
            return new Report($schema, new Statistics($totalRows));
        }

        return null;
    }

    /**
     * Alias for DataFrame::mode.
     *
     * @lazy
     */
    public function saveMode(SaveMode $mode) : self
    {
        return $this->mode($mode);
    }

    /**
     * @trigger
     */
    public function schema() : Schema
    {
        $schema = new Schema();

        foreach ($this->pipeline->process($this->context) as $rows) {
            $schema = $schema->merge($rows->schema());
        }

        return $schema;
    }

    /**
     * @lazy
     * Keep only given entries.
     */
    public function select(string|Reference ...$entries) : self
    {
        $this->pipeline->add(new SelectEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @lazy
     */
    public function sortBy(Reference ...$entries) : self
    {
        $this->pipeline = new LinkedPipeline(new SortingPipeline($this->pipeline, refs(...$entries)));

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
     * @param array<string, ScalarFunction|WindowFunction> $references
     */
    public function withEntries(array $references) : self
    {
        foreach ($references as $entryName => $ref) {
            $this->withEntry($entryName, $ref);
        }

        return $this;
    }

    /**
     * @lazy
     */
    public function withEntry(string|Definition $entry, ScalarFunction|WindowFunction $reference) : self
    {
        if ($reference instanceof WindowFunction) {
            if (\count($reference->window()->partitions())) {
                $this->pipeline = new LinkedPipeline(new PartitioningPipeline($this->pipeline, $reference->window()->partitions(), $reference->window()->order()));
            } else {
                $this->collect();

                if (\count($reference->window()->order())) {
                    $this->sortBy(...$reference->window()->order());
                }
            }

            $this->pipeline->add(new WindowFunctionTransformer($entry, $reference));
        } else {
            $this->transform(new ScalarFunctionTransformer($entry, $reference));
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
