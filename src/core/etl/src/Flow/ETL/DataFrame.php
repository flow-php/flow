<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
use Flow\ETL\Config\Join\JoinAlgorithmBuilder;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\DataFrame\GroupedDataFrame;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Execution\StatisticsCollector;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSteps;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Processor\PartitioningProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SchemaFormatter;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Transformer\AutoCastTransformer;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\Transformer\DropPartitionsTransformer;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\OrderEntries\Comparator;
use Flow\ETL\Transformer\OrderEntries\TypeComparator;
use Flow\ETL\Transformer\OrderEntriesTransformer;
use Flow\ETL\Transformer\Rename\RenameEntryStrategy;
use Flow\ETL\Transformer\RenameEachEntryTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;
use Flow\ETL\Transformer\UntilTransformer;
use Flow\Filesystem\Path\Filter;
use Flow\Types\Type\AutoCaster;
use Generator;
use Throwable;

use function array_merge;
use function array_unshift;
use function count;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\to_output;
use function is_string;

/**
 * @type Aggregations      = list<AggregatingFunction>
 * @type GroupByReferences = list<string|Reference>
 * @type SortReferences    = list<string|Reference>
 */
final class DataFrame
{
    private readonly FlowContext $context;

    public function __construct(
        private Pipeline $pipeline,
        Config|FlowContext $context,
    ) {
        $this->context = $context instanceof FlowContext ? $context : new FlowContext($context);
        $this->context->telemetry()->dataFrameStarted($this->context);
    }

    /**
     * @lazy
     *
     * @param Aggregations $aggregations
     * @param null|GroupByAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function aggregate(array $aggregations, ?GroupByAlgorithmBuilder $algorithm = null): self
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(...$aggregations);

        foreach (GroupBySteps::of($groupBy, $this->context->config, $algorithm) as $step) {
            $this->pipeline->add($step);
        }

        return $this;
    }

    public function autoCast(): self
    {
        $this->pipeline->add(new AutoCastTransformer(new AutoCaster()));

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches but keep those with common value in given column together.
     * This works properly only on sorted datasets.
     *
     * When minSize is not provided, batches will be created only when there is a change in value of the column.
     * When minSize is provided, batches will be created only when there is a change in value of the column or
     * when there are at least minSize rows in the batch.
     *
     * @param Reference|string $column - column to group by (all rows with same value stay together)
     * @param null|int<1, max> $minSize - optional minimum rows per batch for efficiency
     *
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function batchBy(string|Reference $column, ?int $minSize = null): self
    {
        $this->pipeline->add(new BatchingByProcessor(UnresolvedReference::init($column), $minSize));

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches of given size.
     * For example, when Extractor is yielding one row at time, this method will merge them into batches of given size
     * before passing them to the next pipeline element.
     * Similarly when Extractor is yielding batches of rows, this method will split them into smaller batches of given
     * size.
     *
     * In order to merge all Rows into a single batch use DataFrame::collect() method or set size to -1 or 0.
     *
     * @param int<-1, max> $size
     *
     * @lazy
     */
    public function batchSize(int $size): self
    {
        if ($size === -1) {
            return $this->collect();
        }

        if ($size === 0) {
            return $this->collect();
        }

        $this->pipeline->add(new BatchingProcessor($size));

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
     * @param null|Cache $cache reads of this cache must pass the same instance to from_cache()
     *
     * @throws InvalidArgumentException
     */
    public function cache(?string $id = null, ?int $cacheBatchSize = null, ?Cache $cache = null): self
    {
        if ($cacheBatchSize !== null && $cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        if ($cacheBatchSize) {
            $this->pipeline->add(new BatchingProcessor($cacheBatchSize));
        }

        $this->pipeline->add(new CachingProcessor($id, $cache));

        return $this;
    }

    /**
     * Before transforming rows, collect them and merge into single Rows instance.
     * This might lead to memory issues when processing large amount of rows, use with caution.
     *
     * @lazy
     */
    public function collect(): self
    {
        $this->pipeline->add(new CollectingProcessor());

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
    public function collectRefs(References $references): self
    {
        $this->with(new CallbackRowTransformer(static function (Row $row) use ($references): Row {
            foreach ($row->entries()->all() as $entry) {
                $references->add($entry->ref());
            }

            return $row;
        }));

        return $this;
    }

    public function constrain(Constraint $constraint, Constraint ...$constraints): self
    {
        $constraints = array_merge([$constraint], $constraints);

        $this->pipeline->add(new ConstrainedProcessor($constraints));

        return $this;
    }

    /**
     * @trigger
     * Return total count of rows processed by this pipeline.
     */
    public function count(): int
    {
        $total = 0;

        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                $total += $rows->count();
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }

        return $total;
    }

    /**
     * @lazy
     */
    public function crossJoin(self $dataFrame, string $prefix = ''): self
    {
        $this->pipeline->add(new CrossJoinRowsTransformer($dataFrame, $prefix));

        return $this;
    }

    /**
     * @param int $limit maximum numbers of rows to display
     * @param bool|int $truncate false or if set to 0 columns are not truncated, otherwise default truncate to 20
     *                           characters
     * @param Formatter $formatter
     *
     * @trigger
     *
     * @throws InvalidArgumentException
     */
    public function display(
        int $limit = 20,
        int|bool $truncate = 20,
        Formatter $formatter = new AsciiTableFormatter(),
    ): string {
        $this->limit($limit);

        $output = '';

        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                $output .= $formatter->format($rows, $truncate);
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }

        return $output;
    }

    /**
     * Drop given entries.
     *
     * @lazy
     */
    public function drop(string|Reference ...$entries): self
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
    public function dropDuplicates(string|Reference ...$entries): self
    {
        $this->pipeline->add(new DropDuplicatesTransformer(...$entries));

        return $this;
    }

    /**
     * Drop all partitions from Rows, additionally when $dropPartitionColumns is set to true, partition columns are
     * also removed.
     *
     * @lazy
     */
    public function dropPartitions(bool $dropPartitionColumns = false): self
    {
        $this->pipeline->add(new DropPartitionsTransformer($dropPartitionColumns));

        return $this;
    }

    public function duplicateRow(mixed $condition, WithEntry ...$entries): self
    {
        $this->pipeline->add(new DuplicateRowTransformer($condition, ...$entries));

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
    public function fetch(?int $limit = null): Rows
    {
        if ($limit !== null) {
            $this->limit($limit);
        }

        $rows = new Rows();

        try {
            foreach ($this->pipeline->process($this->context) as $nextRows) {
                $rows = $rows->merge($nextRows);
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }

        return $rows;
    }

    /**
     * @lazy
     */
    public function filter(ScalarFunction $function): self
    {
        $this->pipeline->add(new ScalarFunctionFilterTransformer($function));

        return $this;
    }

    /**
     * @lazy
     *
     * @throws RuntimeException
     */
    public function filterPartitions(Filter|ScalarFunction $filter): self
    {
        $extractor = $this->pipeline->extractor();

        if (!$extractor instanceof FileExtractor) {
            throw new RuntimeException(
                'filterPartitions can be used only with extractors that implement FileExtractor interface',
            );
        }

        if ($filter instanceof Filter) {
            $extractor->withPathFilter($filter);

            return $this;
        }

        $extractor->withPathFilter(
            new ScalarFunctionFilter($filter, $this->context->entryFactory(), new AutoCaster(), $this->context),
        );

        return $this;
    }

    /**
     * @lazy
     *
     * @param array<ScalarFunction> $functions
     */
    public function filters(array $functions): self
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
    public function forEach(?callable $callback = null): void
    {
        $this->run($callback);
    }

    /**
     * Yields each row as an instance of Rows.
     *
     * @trigger
     *
     * @return \Generator<Rows>
     */
    public function get(): Generator
    {
        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                yield $rows;
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }
    }

    /**
     * Yields each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array<array<mixed>>>
     */
    public function getAsArray(): Generator
    {
        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                yield $rows->toArray();
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }
    }

    /**
     * Yield each row as an instance of Row.
     *
     * @trigger
     *
     * @return \Generator<Row>
     */
    public function getEach(): Generator
    {
        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                foreach ($rows as $row) {
                    yield $row;
                }
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }
    }

    /**
     * Yield each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array<mixed>>
     */
    public function getEachAsArray(): Generator
    {
        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                foreach ($rows as $row) {
                    yield $row->toArray();
                }
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }
    }

    /**
     * @lazy
     *
     * @param GroupByReferences|Reference|string $entries a single column is grouped by on its own
     * @param null|GroupByAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function groupBy(
        array|Reference|string $entries,
        ?GroupByAlgorithmBuilder $algorithm = null,
    ): GroupedDataFrame {
        $references = is_array($entries) ? $entries : [$entries];

        return new GroupedDataFrame($this, new GroupBy(...$references), $algorithm);
    }

    /**
     * @lazy
     *
     * @param null|JoinAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function join(
        self $dataFrame,
        Expression $on,
        string|Join $type = Join::left,
        ?JoinAlgorithmBuilder $algorithm = null,
    ): self {
        if (is_string($type)) {
            $type = Join::from($type);
        }

        foreach (JoinSteps::of($dataFrame, $on, $type, $this->context->config, $algorithm) as $step) {
            $this->pipeline->add($step);
        }

        return $this;
    }

    /**
     * Joins in memory per batch; it is not governed by the join algorithm and takes no algorithm override.
     *
     * @lazy
     *
     * @psalm-param string|Join $type
     */
    public function joinEach(DataFrameFactory $factory, Expression $on, string|Join $type = Join::left): self
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
    public function limit(?int $limit): self
    {
        if ($limit === null) {
            return $this;
        }

        $this->pipeline = $this->context->config->optimizer()->optimize(new LimitTransformer($limit), $this->pipeline);

        return $this;
    }

    /**
     * @lazy
     */
    public function load(Loader $loader): self
    {
        $this->pipeline = $this->context->config->optimizer()->optimize($loader, $this->pipeline);

        return $this;
    }

    /**
     * @lazy
     *
     * @param callable(Row $row) : Row $callback
     */
    public function map(callable $callback): self
    {
        $this->pipeline->add(new CallbackRowTransformer($callback));

        return $this;
    }

    /**
     * @lazy
     *
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     */
    public function match(Schema $schema, ?SchemaValidator $validator = null): self
    {
        $this->pipeline->add(new SchemaValidationLoader($schema, $validator ?? new StrictValidator()));

        return $this;
    }

    /**
     * Skip given number of rows from the beginning of the dataset.
     * When $offset is null, nothing happens (no rows are skipped).
     *
     * Performance Note: DataFrame must iterate through and process all skipped rows
     * to reach the offset position. For large offsets, this can impact performance
     * as the data source still needs to be read and processed up to the offset point.
     *
     * @param ?int<0, max> $offset
     *
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function offset(?int $offset): self
    {
        if ($offset === null) {
            return $this;
        }

        $this->pipeline->add(new OffsetProcessor($offset));

        return $this;
    }

    /**
     * @lazy
     */
    public function onError(ErrorHandler $handler): self
    {
        $this->context->setErrorHandler($handler);

        return $this;
    }

    /**
     * @lazy
     */
    public function partitionBy(string|Reference $entry, string|Reference ...$entries): self
    {
        array_unshift($entries, $entry);

        $this->pipeline->add(new PartitioningProcessor(References::init(...$entries)->all()));

        return $this;
    }

    /**
     * @trigger
     */
    public function printRows(
        ?int $limit = 20,
        int|bool $truncate = 20,
        Formatter $formatter = new AsciiTableFormatter(),
    ): void {
        if ($limit !== null) {
            $this->limit($limit);
        }

        $this->load(to_output($truncate, Output::rows, $formatter));

        $this->run();
    }

    /**
     * @trigger
     */
    public function printSchema(?int $limit = 20, SchemaFormatter $formatter = new ASCIISchemaFormatter()): void
    {
        if ($limit !== null) {
            $this->limit($limit);
        }
        $this->load(to_output(false, Output::schema, schemaFormatter: $formatter));

        $this->run();
    }

    /**
     * @lazy
     */
    public function rename(string $from, string $to): self
    {
        $this->pipeline->add(new RenameEntryTransformer($from, $to));

        return $this;
    }

    public function renameEach(RenameEntryStrategy ...$strategies): self
    {
        $this->pipeline->add(new RenameEachEntryTransformer(...$strategies));

        return $this;
    }

    public function reorderEntries(Comparator $comparator = new TypeComparator()): self
    {
        $this->pipeline->add(new OrderEntriesTransformer($comparator));

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::transform method.
     */
    public function rows(Transformer|Transformation $transformer): self
    {
        return $this->with($transformer);
    }

    /**
     * @trigger
     *
     * When analyzing pipeline execution we can chose to collect various metrics through analyze()->with*() method
     *
     * - column statistics - analyze()->withColumnStatistics()
     * - schema - analyze()->withSchema()
     *
     * @param null|callable(Rows $rows, FlowContext $context): void $callback
     * @param Analyze|bool $analyze - when set run will return Report
     *
     * @return ($analyze is Analyze|true ? Report : null)
     */
    public function run(?callable $callback = null, bool|Analyze $analyze = false): ?Report
    {
        if ($analyze === false) {
            $analyze = $this->context->config->analyze();
        }

        $collector = new StatisticsCollector($analyze, $this->context);

        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                if ($callback !== null) {
                    $callback($rows, $this->context);
                }

                $collector->capture($rows);
            }

            $collector->end();
        } catch (Throwable $e) {
            $collector->end($e);

            throw $e;
        }

        return $collector->report();
    }

    /**
     * @trigger
     *
     * @return Schema
     */
    public function schema(): Schema
    {
        $schema = new Schema();

        try {
            foreach ($this->pipeline->process($this->context) as $rows) {
                $schema = $schema->merge($rows->schema());
            }
            $this->context->telemetry()->dataFrameCompleted($this->context);
        } catch (Throwable $e) {
            $this->context->telemetry()->dataFrameFailed($this->context, $e);

            throw $e;
        }

        return $schema;
    }

    /**
     * @lazy
     * Keep only given entries.
     */
    public function select(string|Reference ...$entries): self
    {
        $this->pipeline->add(new SelectEntriesTransformer(...$entries));

        return $this;
    }

    /**
     * @lazy
     *
     * @param Reference|SortReferences|string $entries a single column is sorted by on its own
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function sortBy(array|Reference|string $entries, ?SortAlgorithmBuilder $algorithm = null): self
    {
        $references = is_array($entries) ? $entries : [$entries];

        foreach (SortSteps::of(refs(...$references), $this->context->config, $algorithm) as $step) {
            $this->pipeline->add($step);
        }

        return $this;
    }

    /**
     * Alias for DataFrame::with().
     *
     * @lazy
     */
    public function transform(Transformer|Transformation|Transformations|WithEntry $transformer): self
    {
        return $this->with($transformer);
    }

    /**
     * The difference between filter and until is that filter will keep filtering rows until extractors finish yielding
     * rows. Until will send a STOP signal to the Extractor when the condition is not met.
     *
     * @lazy
     */
    public function until(ScalarFunction $function): self
    {
        $this->pipeline->add(new UntilTransformer($function));

        return $this;
    }

    /**
     * @lazy
     * This method is useful mostly in development when
     * you want to pause processing at certain moment without
     * removing code. All operations will get processed up to this point,
     * from here no rows are passed forward.
     */
    public function void(): self
    {
        $this->pipeline->add(new VoidProcessor());

        return $this;
    }

    /**
     * @lazy
     */
    public function with(Transformer|Transformation|Transformations|WithEntry $transformer): self
    {
        if ($transformer instanceof Transformer) {
            $this->pipeline->add($transformer);

            return $this;
        }

        if ($transformer instanceof Transformations) {
            $transformer->transform($this);

            return $this;
        }

        if ($transformer instanceof WithEntry) {
            $this->withEntry($transformer->name, $transformer->function);

            return $this;
        }

        return $transformer->transform($this);
    }

    /**
     * @lazy
     *
     * @param array<int, WithEntry>|array<string, ScalarFunction|WindowFunction|WithEntry> $references
     */
    public function withEntries(array $references): self
    {
        foreach ($references as $entryName => $ref) {
            if ($ref instanceof WithEntry) {
                $this->withEntry($ref->name, $ref->function);
            } else {
                $this->withEntry((string) $entryName, $ref);
            }
        }

        return $this;
    }

    /**
     * @param Definition<mixed>|string $entry
     *
     * @lazy
     */
    public function withEntry(string|Definition $entry, ScalarFunction|WindowFunction $reference): self
    {
        if ($reference instanceof WindowFunction) {
            if (count($reference->window()->partitions())) {
                $this->pipeline->add(
                    new PartitioningProcessor($reference->window()->partitions(), $reference->window()->order()),
                );
            }

            $this->pipeline->add(new WindowProcessor($entry, $reference));
        } else {
            $this->with(new ScalarFunctionTransformer($entry, $reference));
        }

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::load function.
     */
    public function write(Loader $loader): self
    {
        return $this->load($loader);
    }
}
