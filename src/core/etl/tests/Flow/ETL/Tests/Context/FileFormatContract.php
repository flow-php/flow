<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Path;
use PHPUnit\Framework\Assert;
use Throwable;

use function array_column;
use function array_map;
use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_instance_of;
use function iterator_to_array;
use function range;

use const PHP_INT_MAX;

/**
 * The write and read cases every file format runs, so a fix in one loader or extractor that the others lack shows up
 * as a red format. Each case writes under $cacheDir, which the calling test owns.
 */
final readonly class FileFormatContract
{
    public function __construct(
        private Path $cacheDir,
    ) {}

    public function overwriteReplacesWhatTheDestinationHeld(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('/out/data.' . $format->extension());

        df()
            ->read(from_array([['text' => 'a'], ['text' => 'b']]))
            ->write($format->loader($destination))
            ->run();
        df()
            ->read(from_array([['text' => 'c'], ['text' => 'd'], ['text' => 'e']]))
            ->write($format->loader($destination)->saveMode(overwrite()))
            ->run();

        Assert::assertSame(3, df()->read($format->extractor($this->written($format)))->count());
    }

    public function appendKeepsWhatTheDestinationHeld(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('/out/data.' . $format->extension());

        df()
            ->read(from_array([['text' => 'a'], ['text' => 'b']]))
            ->write($format->loader($destination))
            ->run();
        df()
            ->read(from_array([['text' => 'c'], ['text' => 'd'], ['text' => 'e']]))
            ->write($format->loader($destination)->saveMode(append()))
            ->run();

        Assert::assertSame(5, df()->read($format->extractor($this->written($format)))->count());
    }

    public function ignoreLeavesAnExistingDestinationUntouched(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('/out/data.' . $format->extension());

        df()
            ->read(from_array([['text' => 'a'], ['text' => 'b']]))
            ->write($format->loader($destination))
            ->run();
        $written = native_local_filesystem()->readFrom($destination)->content();
        df()
            ->read(from_array([['text' => 'c'], ['text' => 'd'], ['text' => 'e']]))
            ->write($format->loader($destination)->saveMode(ignore()))
            ->run();

        Assert::assertSame($written, native_local_filesystem()->readFrom($destination)->content());
        Assert::assertSame(2, df()->read($format->extractor($this->written($format)))->count());
    }

    public function exceptionIfExistsRefusesAnExistingDestination(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('/out/data.' . $format->extension());

        df()
            ->read(from_array([['text' => 'a']]))
            ->write($format->loader($destination))
            ->run();

        try {
            df()
                ->read(from_array([['text' => 'b']]))
                ->write($format->loader($destination))
                ->run();
        } catch (RuntimeException $refused) {
            Assert::assertStringContainsString('already exists', $refused->getMessage());

            return;
        }

        Assert::fail($format->extension() . ' wrote over an existing destination');
    }

    public function aBatchOfZeroRowsCreatesNoFile(FileFormat $format): void
    {
        $loader = $format->loader($this->cacheDir->suffix('/out/data.' . $format->extension()));

        $loader->load(array_to_rows([], schema(str_schema('text'))), flow_context()->backend());
        $loader->closure();

        Assert::assertSame(
            [],
            iterator_to_array(native_local_filesystem()->list($this->cacheDir->suffix('/out/**/*')), false),
        );
    }

    public function aPartitionedWriteReadsBackEveryPartition(FileFormat $format): void
    {
        df()
            ->read(from_array([['text' => 'a', 'p' => 'x'], ['text' => 'b', 'p' => 'y'], ['text' => 'c', 'p' => 'y']]))
            ->write(
                $format
                    ->loader($this->cacheDir->suffix('/out/data.' . $format->extension()))
                    ->partitionBy(partition_by('p')),
            )
            ->run();

        Assert::assertSame(
            ['x', 'y', 'y'],
            array_column(
                df()
                    ->read($format->extractor($this->cacheDir->suffix('/out/**/*.' . $format->extension())))
                    ->sortBy(ref('p'))
                    ->fetch()
                    ->toArray(),
                'p',
            ),
        );
    }

    public function aLoaderInATaskSurvivesDescribe(FileFormat $format): void
    {
        $loader = $format
            ->loader($this->cacheDir->suffix('/out/data.' . $format->extension()))
            ->saveMode(overwrite())
            ->inTask(new Part(1, 2, 'run1', false));

        Assert::assertEquals($loader->describe(), DescribeRoundTrip::rebuilt($loader)->describe());
    }

    public function anExtractorReaderSurvivesDescribe(FileFormat $format): void
    {
        df()
            ->read(from_array([['text' => 'a']]))
            ->write($format->loader($this->cacheDir->suffix('/out/data.' . $format->extension())))
            ->run();
        $reader = type_instance_of(Describable::class)->assert(
            $format->extractor($this->written($format))->withBatchSize(7)->withMetadataColumns(true)->reader(),
        );

        Assert::assertEquals($reader->describe(), DescribeRoundTrip::rebuilt($reader)->describe());
    }

    public function theSplitsReadEveryRowOnce(FileFormat $format): void
    {
        df()
            ->read(from_array(array_map(static fn(int $i): array => ['text' => 'row-' . $i], range(1, 50))))
            ->write($format->loader($this->cacheDir->suffix('/out/data.' . $format->extension())))
            ->run();
        $extractor = $format->extractor($this->written($format));
        $read = 0;

        foreach ($extractor->splits(new Criteria(), 1) as $split) {
            foreach (FlowTestCase::extracted(
                $extractor->reader(),
                flow_context()->backend(),
                (new Criteria())->withSplit(type_instance_of(FileSplit::class)->assert($split)),
            ) as $rows) {
                $read += $rows->count();
            }
        }

        Assert::assertSame(50, $read);
    }

    public function metadataColumnsNameTheSourceFile(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('/out/data.' . $format->extension());

        df()
            ->read(from_array([['text' => 'a']]))
            ->write($format->loader($destination))
            ->run();

        Assert::assertSame(
            [$destination->uri()],
            array_column(
                df()->read($format->extractor($destination)->withMetadataColumns(true))->fetch()->toArray(),
                '_input_file_uri',
            ),
        );
    }

    public function theBatchSizeBoundsEveryBatch(FileFormat $format): void
    {
        df()
            ->read(from_array([['text' => 'a'], ['text' => 'b'], ['text' => 'c'], ['text' => 'd'], ['text' => 'e']]))
            ->write($format->loader($this->cacheDir->suffix('/out/data.' . $format->extension())))
            ->run();

        Assert::assertSame(
            [2, 2, 1],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array(
                    df()->read($format->extractor($this->written($format))->withBatchSize(2))->get(),
                    false,
                ),
            ),
        );
    }

    public function refreshListsTheFilesWrittenAfterARead(FileFormat $format): void
    {
        df()
            ->read(from_array([['text' => 'a']]))
            ->write($format->loader($this->cacheDir->suffix('/out/first.' . $format->extension())))
            ->run();
        $extractor = $format->extractor($this->written($format));
        $extractor->splits(new Criteria(), PHP_INT_MAX);
        df()
            ->read(from_array([['text' => 'b']]))
            ->write($format->loader($this->cacheDir->suffix('/out/second.' . $format->extension())))
            ->run();

        $extractor->refresh();

        Assert::assertCount(2, $extractor->splits(new Criteria(), PHP_INT_MAX));
    }

    /**
     * A format writer emits its footer when it closes, so closing a dead run's writer turns a half-written file into
     * a complete, readable one. Whatever the save mode, that file must not be left at the destination.
     */
    public function aFailedRunLeavesNoFileAtTheDestination(FileFormat $format): void
    {
        $destination = $this->cacheDir->suffix('aborted.' . $format->extension());

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
                ->batchSize(1)
                ->with(new ThrowWhenRowMatches('id', 3, new RuntimeException('aborted')))
                ->write($format->loader($destination))
                ->run();
            Assert::fail('The run was expected to fail');
        } catch (Throwable $e) {
            Assert::assertSame('aborted', $e->getMessage());
        }

        Assert::assertNull(native_local_filesystem()->status($destination));
    }

    /**
     * $predicate keeps two of the ids "a", "b", "c" written in one file: the format names the column it reads them as.
     */
    public function aPushedFilterReturnsWhatTheFilterNodeReturns(FileFormat $format, ScalarFunction $predicate): void
    {
        $file = $this->cacheDir->suffix('/pushed/rows.' . $format->extension());

        df()
            ->read(from_array([['id' => 'a'], ['id' => 'b'], ['id' => 'c']]))
            ->write($format->loader($file))
            ->run();

        $pushed = df()->read($format->extractor($file))->filter($predicate)->fetch();
        $plain = df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
            ->read($format->extractor($file))
            ->filter($predicate)
            ->fetch();

        Assert::assertCount(2, $plain);
        Assert::assertEquals($plain->toArray(), $pushed->toArray());
    }

    public function readsBackThroughTheSameFilesystemInstance(FileFormat $format): void
    {
        $shared = memory_filesystem();
        $file = path('memory://family.' . $format->extension());

        df()
            ->read(from_array([['id' => 'a'], ['id' => 'b']]))
            ->write($format->loader($file, $shared))
            ->run();

        $rows = [];
        df()->read($format->extractor($file, $shared))->write(to_array($rows))->run();

        Assert::assertNotSame([], $rows, 'the source must read back what the sink wrote to the same instance');
    }

    public function anExtractorRejectsAPathItsFilesystemDoesNotServe(FileFormat $format): void
    {
        try {
            $format->extractor(path('aws-s3://bucket/orders.dat'));
        } catch (InvalidArgumentException $refused) {
            Assert::assertStringContainsString(self::notServed(), $refused->getMessage());

            return;
        }

        Assert::fail($format->extension() . ' extractor took a path its filesystem does not serve');
    }

    public function aLoaderRejectsAPathItsFilesystemDoesNotServe(FileFormat $format): void
    {
        try {
            $format->loader(path('aws-s3://bucket/orders.dat'));
        } catch (InvalidArgumentException $refused) {
            Assert::assertStringContainsString(self::notServed(), $refused->getMessage());

            return;
        }

        Assert::fail($format->extension() . ' loader took a path its filesystem does not serve');
    }

    public static function notServed(): string
    {
        return (
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.dat".'
        );
    }

    public function written(FileFormat $format): Path
    {
        return $this->cacheDir->suffix('/out/*.' . $format->extension());
    }
}
