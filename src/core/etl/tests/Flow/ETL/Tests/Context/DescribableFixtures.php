<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Closure;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Functions\DenseRank;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\Functions\Join\Condition\All;
use Flow\ETL\Functions\Join\Condition\Any;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Condition\Identical;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Functions\Rank;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Functions\RowNumber;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Functions\Window;
use Flow\ETL\Functions\Window\FrameBound;
use Flow\ETL\Functions\Window\FrameBoundType;
use Flow\ETL\Functions\Window\PeerFrame;
use Flow\ETL\Functions\Window\RowsFrame;
use Flow\ETL\Functions\Window\WholePartitionFrame;
use Flow\ETL\Functions\WithColumn;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Floe\FloeFileBatches;
use Flow\ETL\IO\Floe\FloeLoader;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Loader\ArrayLoader;
use Flow\ETL\IO\Loader\MemoryLoader;
use Flow\ETL\IO\Loader\SchemaValidationLoader;
use Flow\ETL\IO\Loader\StreamLoader;
use Flow\ETL\IO\Loader\StreamLoader\Output;
use Flow\ETL\IO\Loader\StreamLoader\Type;
use Flow\ETL\Kernels\Hash\NativePHPHash;
use Flow\ETL\Kernels\Hash\PlainText;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\MergedAggregate;
use Flow\ETL\Operators\GroupBy\PartialAggregate;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Processor\ExpandingProcessor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Operators\Transformer\CollectReferencesTransformer;
use Flow\ETL\Operators\Transformer\DropColumnsTransformer;
use Flow\ETL\Operators\Transformer\DuplicateRowTransformer;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\PruneColumnsTransformer;
use Flow\ETL\Operators\Transformer\Rename\RenameCaseColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameMapColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameReplaceColumnStrategy;
use Flow\ETL\Operators\Transformer\RenameColumnTransformer;
use Flow\ETL\Operators\Transformer\RenameEachColumnTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Operators\Transformer\SelectColumnsTransformer;
use Flow\ETL\Operators\Transformer\SerializeTransformer;
use Flow\ETL\Operators\Transformer\UnserializeTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\SkipRows;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\Executor\OfferedFailure;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\Serializer\CompressingSerializer;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Schema\Validator\EvolvingValidator;
use Flow\ETL\Values\Schema\Validator\SelectiveValidator;
use Flow\ETL\Values\Schema\Validator\StrictValidator;
use Flow\ETL\Values\SortOrder;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Stream\Mode;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

/**
 * One instance of every Describable etl ships, with every constructor argument away from its default, so a parameter
 * that describe() or fromDescription() forgets changes the rebuilt object. The helpers several packages share stay
 * here; each package's own Tests\Context\DescribableFixtures holds its classes.
 */
final class DescribableFixtures
{
    /**
     * @return array<class-string<Describable>, Closure(): Describable>
     */
    public static function all(): array
    {
        $fixtures = [];

        foreach (ScalarFunctionFixtures::all() as $class => $fixture) {
            $fixtures[$class] = $fixture['factory'](...);
        }

        foreach (AggregatorFixtures::classes() as $class) {
            $fixtures[$class] = static fn(): Describable => AggregatorFixtures::instance($class);
        }

        return [...$fixtures, ...self::others()];
    }

    /**
     * The classes whose describe() is null for every instance: they hold a live client, a closure or the runtime.
     *
     * @return list<class-string<Describable>>
     */
    public static function undescribable(): array
    {
        return [
            ArrayLoader::class,
            CachingProcessor::class,
            ConstrainedProcessor::class,
            DuplicateRowTransformer::class,
            JoinEachRowsTransformer::class,
            MemoryLoader::class,
            OfferedFailure::class,
            StreamLoader::class,
        ];
    }

    /**
     * The classes whose constructor always throws: the Avro integration was abandoned.
     *
     * @return list<class-string<Describable>>
     */
    public static function unconstructable(): array
    {
        return [];
    }

    public static function arrayLoader(): ArrayLoader
    {
        $array = [['id' => 1]];

        return new ArrayLoader($array);
    }

    public static function mountedFilesystem(): NativeLocalFilesystem
    {
        return new NativeLocalFilesystem(new Mount('local'));
    }

    public static function fileRead(): FileRead
    {
        return new FileRead(
            new SourceFiles(new NativeLocalFilesystem(), path('/data/orders/*.csv')),
            new FileColumns(['country' => true], new PartitionTypes(['country' => type_string()]), true),
            schema(int_schema('id'), str_schema('country')),
            7,
        );
    }

    public static function window(): Window
    {
        return new Window(
            [ref('country')],
            [ref('id')->desc()],
            new RowsFrame(new FrameBound(FrameBoundType::PRECEDING, 2), new FrameBound(FrameBoundType::CURRENT_ROW)),
        );
    }

    public static function bucketing(): BucketingConfig
    {
        return new BucketingConfig(new MemoryRowsStorage(), 3, 11, Unit::fromKb(5));
    }

    public static function hashJoin(): HashJoin
    {
        return new HashJoin(
            join_on(['id' => 'customer_id'], 'c_'),
            JoinType::left,
            new HashJoinConfig(self::bucketing(), Unit::fromKb(9)),
            new NativePHPRandomValueGenerator(),
            schema(int_schema('id')),
            schema(int_schema('customer_id'), str_schema('name')),
        );
    }

    public static function filesystemBuckets(): FilesystemRowsStorage
    {
        return new FilesystemRowsStorage(new NativeLocalFilesystem(), path('/tmp/flow-buckets'), new PhpBackend(), 17);
    }

    /**
     * @return array<class-string<Describable>, Closure(): Describable>
     */
    public static function others(): array
    {
        return [
            AddRowIndexTransformer::class => static fn(): Describable => new AddRowIndexTransformer(
                'position',
                StartFrom::ZERO,
            ),
            AdaptiveBackend::class => static fn(): Describable => new AdaptiveBackend(),
            ArrayLoader::class => self::arrayLoader(...),
            BucketingConfig::class => self::bucketing(...),
            CollectReferencesTransformer::class => static fn(): Describable => new CollectReferencesTransformer(refs(
                ref('id'),
                ref('name'),
            )),
            CompressingSerializer::class => static fn(): Describable => new CompressingSerializer(
                new FloeSerializer(new PhpBackend()),
                3,
            ),
            ConfiguredFilesystem::class => static fn(): Describable => new ConfiguredFilesystem(
                new NativeLocalFilesystem(),
            ),
            DenseRank::class => static fn(): Describable => new DenseRank(self::window()),
            DropColumnsTransformer::class => static fn(): Describable => new DropColumnsTransformer('id', ref('name')),
            DuplicateRowTransformer::class => static fn(): Describable => new DuplicateRowTransformer(
                ref('id')->greaterThan(lit(1)),
                [new WithColumn('copy', lit(true))],
            ),
            Equal::class => static fn(): Describable => new Equal(ref('id'), 'customer_id'),
            EvolvingValidator::class => static fn(): Describable => new EvolvingValidator(),
            Expression::class => static fn(): Describable => new Expression(new Identical('id', 'customer_id'), 'c_'),
            ExternalSortConfig::class => static fn(): Describable => new ExternalSortConfig(
                self::bucketing(),
                self::filesystemBuckets(),
            ),
            FileSplit::class => static fn(): Describable => new FileSplit(path('/data/a.csv'), 3, 100, 400),
            FileSplitReader::class => static fn(): Describable => new FileSplitReader(
                self::fileRead(),
                new FloeFileBatches(new NoopCodec(), 512, new DerivedSchema(schema(int_schema('id')), 'a.floe')),
            ),
            FilesystemRowsStorage::class => self::filesystemBuckets(...),
            Filter::class => static fn(): Describable => Filter::bind(
                schema(int_schema('id')),
                ref('id')->greaterThan(lit(1)),
            ),
            FloeLoader::class => static fn(): Describable => new FloeLoader(
                path('local://data/out.floe'),
                schema_metadata(['source' => 'orders']),
                new Options(1024, new NoopCodec()),
                self::mountedFilesystem(),
            ),
            FloeFileBatches::class => static fn(): Describable => new FloeFileBatches(
                new NoopCodec(),
                512,
                new DerivedSchema(schema(int_schema('id')), 'a.floe'),
            ),
            FloeSerializer::class => static fn(): Describable => new FloeSerializer(new PhpBackend(), 13),
            GroupBy::class => static fn(): Describable => new GroupBy(
                refs('country', ref('day')),
                new Aggregators(sum(ref('amount'))),
            ),
            HashJoinConfig::class => static fn(): Describable => new HashJoinConfig(self::bucketing(), Unit::fromKb(9)),
            IgnoreError::class => static fn(): Describable => new IgnoreError(),
            JoinEachRowsTransformer::class => static fn(): Describable => JoinEachRowsTransformer::left(
                new StaticExtractorFactory(from_array([['id' => 1]])),
                join_on(['id' => 'id'], 'r_'),
            ),
            LimitTransformer::class => static fn(): Describable => new LimitTransformer(13),
            MemoryRowsStorage::class => static fn(): Describable => new MemoryRowsStorage(),
            MemoryLoader::class => static fn(): Describable => new MemoryLoader(new ArrayMemory()),
            MemorySortConfig::class => static fn(): Describable => new MemorySortConfig(),
            MergedAggregate::class => static fn(): Describable => new MergedAggregate(sum(ref('amount'))),
            NativePHPHash::class => static fn(): Describable => new NativePHPHash('sha256', true, ['seed' => 3]),
            NativePHPRandomValueGenerator::class => static fn(): Describable => new NativePHPRandomValueGenerator(),
            PartialAggregate::class => static fn(): Describable => new PartialAggregate(sum(ref('amount'))),
            PeerFrame::class => static fn(): Describable => new PeerFrame([ref('id')->desc()]),
            PhpBackend::class => static fn(): Describable => new PhpBackend(),
            PlainText::class => static fn(): Describable => new PlainText(),
            PruneColumnsTransformer::class => static fn(): Describable => new PruneColumnsTransformer(
                'id',
                ref('name'),
            ),
            Rank::class => static fn(): Describable => new Rank(self::window()),
            References::class => static fn(): Describable => refs(ref('id'), ref('name')->as('label')),
            RenameCaseColumnStrategy::class => static fn(): Describable => new RenameCaseColumnStrategy(
                StringStyles::SNAKE,
            ),
            RenameColumnTransformer::class => static fn(): Describable => new RenameColumnTransformer('id', 'key'),
            RenameEachColumnTransformer::class => static fn(): Describable => new RenameEachColumnTransformer(
                new RenameMapColumnStrategy(['id' => 'key']),
                new RenameReplaceColumnStrategy(['_'], ['-']),
            ),
            RenameMapColumnStrategy::class => static fn(): Describable => new RenameMapColumnStrategy(['id' => 'key']),
            RenameReplaceColumnStrategy::class => static fn(): Describable => new RenameReplaceColumnStrategy(['_'], [
                '-',
            ]),
            RowNumber::class => static fn(): Describable => new RowNumber(self::window()),
            RowsFrame::class => static fn(): Describable => new RowsFrame(
                new FrameBound(FrameBoundType::PRECEDING, 2),
                new FrameBound(FrameBoundType::FOLLOWING, 1),
            ),
            ScalarFunctionFilterTransformer::class => static fn(): Describable => new ScalarFunctionFilterTransformer(
                ref('id')->greaterThan(lit(1)),
                Filter::bind(schema(int_schema('id')), ref('id')->greaterThan(lit(1))),
            ),
            ScalarFunctionTransformer::class => static fn(): Describable => type_instance_of(
                ScalarFunctionTransformer::class,
            )->assert((new ScalarFunctionTransformer('double', ref('id')->multiply(lit(2))))->bind(schema(int_schema(
                'id',
            )))->step),
            SchemaValidationLoader::class => static fn(): Describable => new SchemaValidationLoader(
                schema(int_schema('id'), bool_schema('active')),
                new SelectiveValidator(),
            ),
            SelectColumnsTransformer::class => static fn(): Describable => new SelectColumnsTransformer(
                'id',
                ref('name'),
            ),
            SelectiveValidator::class => static fn(): Describable => new SelectiveValidator(),
            SerializeTransformer::class => static fn(): Describable => new SerializeTransformer(
                ref('payload'),
                config()->serializer(),
            ),
            OfferedFailure::class => static fn(): Describable => new OfferedFailure(new SkipRows()),
            SkipRows::class => static fn(): Describable => new SkipRows(),
            SortColumn::class => static fn(): Describable => new SortColumn('id', SortOrder::DESC, NullsOrder::FIRST),
            StreamLoader::class => static fn(): Describable => new StreamLoader(
                'php://stderr',
                Mode::APPEND_WRITE,
                false,
                Output::rows_and_schema,
                type: Type::stderr,
            ),
            StrictValidator::class => static fn(): Describable => new StrictValidator(),
            ThrowError::class => static fn(): Describable => new ThrowError(),
            UnserializeTransformer::class => static fn(): Describable => new UnserializeTransformer(
                ref('payload'),
                schema(int_schema('id')),
                config()->serializer(),
                'p_',
            ),
            UntilTransformer::class => static fn(): Describable => type_instance_of(
                UntilTransformer::class,
            )->assert((new UntilTransformer(ref('id')->lessThan(lit(10))))->bind(schema(int_schema('id')))->step),
            Window::class => self::window(...),
            WholePartitionFrame::class => static fn(): Describable => new WholePartitionFrame(),
            BatchingByProcessor::class => static fn(): Describable => new BatchingByProcessor(ref('country'), 5),
            BatchingProcessor::class => static fn(): Describable => new BatchingProcessor(new BatchSize(7)),
            CachingProcessor::class => static fn(): Describable => new CachingProcessor(
                'cache-id',
                new MemoryRowsStorage(),
            ),
            CollectingProcessor::class => static fn(): Describable => new CollectingProcessor(schema(int_schema('id'))),
            ConstrainedProcessor::class => static fn(): Describable => new ConstrainedProcessor(),
            CountingProcessor::class => static fn(): Describable => new CountingProcessor(),
            DistinctProcessor::class => static fn(): Describable => type_instance_of(
                DistinctProcessor::class,
            )->assert((new DistinctProcessor(['country', ref('day')]))->bind(schema(
                str_schema('country'),
                int_schema('day'),
            ))->step),
            ExpandingProcessor::class => static fn(): Describable => type_instance_of(
                ExpandingProcessor::class,
            )->assert((new ExpandingProcessor(
                'tag',
                ref('tags')->expand(),
                RequiredColumns::allBut('tags'),
                new BatchSize(9),
            ))->bind(schema(int_schema('id'), list_schema('tags', type_list(type_string()))))->step),
            GroupByAggregationProcessor::class => static fn(): Describable => type_instance_of(
                GroupByAggregationProcessor::class,
            )->assert((new GroupByAggregationProcessor(
                new GroupBy(refs('country'), new Aggregators(sum(ref('amount')))),
                self::bucketing(),
                new NativePHPRandomValueGenerator(),
            ))->bind(schema(str_schema('country'), int_schema('amount')))->step),
            HashJoin::class => self::hashJoin(...),
            MemorySortProcessor::class => static fn(): Describable => new MemorySortProcessor([ref(
                'id',
            )->desc()], schema(int_schema('id'))),
            MergeSortProcessor::class => static fn(): Describable => new MergeSortProcessor(
                new ExternalSortConfig(self::bucketing()),
                [ref('id')->asc()],
                new NativePHPRandomValueGenerator(),
            ),
            OffsetProcessor::class => static fn(): Describable => new OffsetProcessor(3),
            PivotProcessor::class => static fn(): Describable => type_instance_of(
                PivotProcessor::class,
            )->assert((new PivotProcessor(
                new GroupBy(
                    refs('name'),
                    new Aggregators(sum(ref('id'))),
                    new Pivot(ref('country'), new DeclaredPivotValues('USA', 'China')),
                ),
                self::bucketing(),
            ))->bind(schema(str_schema('name'), int_schema('id'), str_schema('country')))->step),
            RepartitionProcessor::class => static fn(): Describable => new RepartitionProcessor(
                refs('country'),
                self::bucketing(),
                new NativePHPRandomValueGenerator(),
            ),
            TopNProcessor::class => static fn(): Describable => new TopNProcessor(
                [ref('id')->desc()],
                4,
                new ExternalSortConfig(self::bucketing()),
                new NativePHPRandomValueGenerator(),
                schema(int_schema('id')),
            ),
            VoidProcessor::class => static fn(): Describable => new VoidProcessor(schema(int_schema('id'))),
            WindowProcessor::class => static fn(): Describable => type_instance_of(
                WindowProcessor::class,
            )->assert((new WindowProcessor('rank', new Rank(self::window())))->bind(schema(
                str_schema('country'),
                int_schema('id'),
            ))->step),
            All::class => static fn(): Describable => new All(new Equal('id', 'id'), new Identical('name', 'name')),
            Any::class => static fn(): Describable => new Any(new Equal('id', 'id'), new Identical('name', 'name')),
            Identical::class => static fn(): Describable => new Identical(ref('id'), 'customer_id'),
        ];
    }

    public static function inferredColumns(): InferredColumns
    {
        return new InferredColumns(
            schema(int_schema('id')),
            ['country'],
            new SchemaInference(7, 3, InferredTypes::default(), true),
            'a.csv',
        );
    }
}
