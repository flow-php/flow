<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function array_column;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class FileSourceFilesystemTest extends FlowIntegrationTestCase
{
    #[TestWith(['json'])]
    #[TestWith(['json_lines'])]
    #[TestWith(['text'])]
    #[TestWith(['xml'])]
    #[TestWith(['excel'])]
    public function test_each_family_reads_back_through_the_same_filesystem_instance(string $family): void
    {
        $shared = memory_filesystem();
        $uri = path('memory://family-' . $family . '.' . $family);

        [$sink, $source] = match ($family) {
            'json' => [to_json($uri, filesystem: $shared), from_json($uri, filesystem: $shared)],
            'json_lines' => [to_json_lines($uri, filesystem: $shared), from_json_lines($uri, filesystem: $shared)],
            'text' => [to_text($uri, filesystem: $shared), from_text($uri, filesystem: $shared)],
            'xml' => [to_xml($uri, filesystem: $shared), from_xml($uri, filesystem: $shared)],
            default => [null, null],
        };

        if ($sink === null || $source === null) {
            // OpenSpout drives ZipArchive, which takes a local path and cannot read a memory:// stream, so the
            // read itself never crosses the Filesystem. The one call that does is the format sniff, and only an
            // extension-less path needs it - a *.xlsx path is typed by its extension and opens nothing.
            $written = path($this->cacheDir->path() . '/family-excel.xlsx');
            $sniffed = path($this->cacheDir->path() . '/family-excel');
            $counting = new CountingFilesystem($this->fs);

            df()
                ->read(from_array([['id' => 'a']]))
                ->write(to_excel($written))
                ->run();

            $this->fs->mv($written, $sniffed);

            $rows = [];
            df()->read(from_excel($sniffed, filesystem: $counting))->write(to_array($rows))->run();

            static::assertSame(['a'], array_column($rows, 'id'));
            // twice: an undeclared read sniffs the format once for the schema sample and once for the read loop
            static::assertSame(2, $counting->readFromCalls);

            return;
        }

        df()
            ->read(from_array([['id' => 'a'], ['id' => 'b']]))
            ->write($sink)
            ->run();

        $rows = [];
        df()->read($source)->write(to_array($rows))->run();

        static::assertNotSame([], $rows, 'the source must read back what the sink wrote to the same instance');
    }

    /**
     * Every source and sink guards its (Filesystem, Path) pair at construction. Avro is excluded on both
     * sides: its constructors throw unconditionally, so a guard after that throw would be unreachable.
     *
     * @param array{string, string} $dsl namespace and function name of the DSL constructor
     */
    #[TestWith([['Flow\ETL\Adapter\CSV', 'from_csv']])]
    #[TestWith([['Flow\ETL\Adapter\JSON', 'from_json']])]
    #[TestWith([['Flow\ETL\Adapter\JSON', 'from_json_lines']])]
    #[TestWith([['Flow\ETL\Adapter\Text', 'from_text']])]
    #[TestWith([['Flow\ETL\Adapter\Parquet', 'from_parquet']])]
    #[TestWith([['Flow\ETL\Adapter\XML', 'from_xml']])]
    #[TestWith([['Flow\ETL\Adapter\Excel\DSL', 'from_excel']])]
    #[TestWith([['Flow\Floe\DSL', 'from_floe']])]
    #[TestWith([['Flow\ETL\DSL', 'files']])]
    #[TestWith([['Flow\ETL\DSL', 'from_path_partitions']])]
    #[TestWith([['Flow\ETL\Adapter\CSV', 'to_csv']])]
    #[TestWith([['Flow\ETL\Adapter\JSON', 'to_json']])]
    #[TestWith([['Flow\ETL\Adapter\JSON', 'to_json_lines']])]
    #[TestWith([['Flow\ETL\Adapter\Text', 'to_text']])]
    #[TestWith([['Flow\ETL\Adapter\Parquet', 'to_parquet']])]
    #[TestWith([['Flow\ETL\Adapter\XML', 'to_xml']])]
    #[TestWith([['Flow\ETL\Adapter\Excel\DSL', 'to_excel']])]
    #[TestWith([['Flow\Floe\DSL', 'to_floe']])]
    public function test_every_family_rejects_a_path_its_filesystem_does_not_serve(array $dsl): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.dat".',
        );

        $construct = $dsl[0] . '\\' . $dsl[1];
        $construct(path('aws-s3://bucket/orders.dat'));
    }

    public function test_floe_opens_each_file_once(): void
    {
        $counting = new CountingFilesystem($this->fs);
        $uri = $this->cacheDir->path() . '/once.floe';

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_floe(path($uri)))
            ->run();

        $rows = [];
        df()
            ->read(from_floe(path($uri), filesystem: $counting))
            ->write(to_array($rows))
            ->run();

        static::assertSame([1, 2], array_column($rows, 'id'));
        static::assertSame(1, $counting->readFromCalls, 'a listed file must be opened exactly once');
    }

    public function test_floe_schema_takes_no_arguments(): void
    {
        $uri = $this->cacheDir->path() . '/schema-arg.floe';

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe(path($uri)))
            ->run();

        // a file source is self-sufficient about its own source: schema() needs no run and no context
        $schema = from_floe(path($uri), filesystem: $this->fs)->schema();

        static::assertNotNull($schema->findDefinition('id'));
    }

    public function test_listing_opens_no_streams_on_path_partitions(): void
    {
        $counting = new CountingFilesystem($this->fs);

        $this->givenCsv($this->cacheDir->path() . '/partitions/date=2024-01-01/a.csv');
        $this->givenCsv($this->cacheDir->path() . '/partitions/date=2024-01-02/b.csv');

        $rows = [];
        df()
            ->read(from_path_partitions(path($this->cacheDir->path() . '/partitions/**/*.csv'), filesystem: $counting))
            ->write(to_array($rows))
            ->run();

        static::assertCount(2, $rows);
        static::assertSame(0, $counting->readFromCalls, 'listing must open nothing');
    }

    public function test_reading_a_remote_path_without_a_filesystem_throws_naming_the_argument(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.csv". Pass the filesystem that handles this scheme, '
            . 'e.g. from_csv($path, filesystem: aws_s3_filesystem(...)).',
        );

        from_csv(path('aws-s3://bucket/orders.csv'));
    }

    public function test_a_shared_memory_filesystem_round_trips_but_two_calls_do_not(): void
    {
        // two memory_filesystem() calls are two separate stores; only a shared instance round-trips
        $shared = memory_filesystem();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_csv(path('memory://s1.csv'), filesystem: $shared))
            ->run();

        $paired = [];
        df()
            ->read(from_csv(path('memory://s1.csv'), filesystem: $shared))
            ->write(to_array($paired))
            ->run();

        static::assertSame([1, 2], array_column($paired, 'id'));

        $unpaired = [];
        df()
            ->read(from_csv(path('memory://s1.csv'), filesystem: memory_filesystem()))
            ->write(to_array($unpaired))
            ->run();

        static::assertSame([], $unpaired);
    }

    public function test_writing_a_remote_path_without_a_filesystem_throws_naming_the_argument(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.csv". Pass the filesystem that handles this scheme, '
            . 'e.g. to_csv($path, filesystem: aws_s3_filesystem(...)).',
        );

        to_csv(path('aws-s3://bucket/orders.csv'));
    }

    public function test_reads_through_the_given_filesystem(): void
    {
        $counting = new CountingFilesystem($this->fs);
        $uri = $this->cacheDir->path() . '/orders.csv';

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_csv(path($uri)))
            ->run();

        $rows = [];
        df()
            ->read(from_csv(path($uri), filesystem: $counting))
            ->write(to_array($rows))
            ->run();

        static::assertSame([1, 2], array_column($rows, 'id'));
        static::assertSame(
            3,
            $counting->readFromCalls,
            'header() + the sample pass + the read, all through the filesystem it was given',
        );
    }

    public function test_the_files_source_lists_through_the_given_filesystem(): void
    {
        $counting = new CountingFilesystem($this->fs);

        $this->givenCsv($this->cacheDir->path() . '/listing/a.csv');
        $this->givenCsv($this->cacheDir->path() . '/listing/b.csv');

        $rows = [];
        df()
            ->read(files(path($this->cacheDir->path() . '/listing/*.csv'), filesystem: $counting))
            ->write(to_array($rows))
            ->run();

        static::assertCount(2, $rows);
        static::assertSame(0, $counting->readFromCalls, 'listing must open nothing');
    }

    public function givenCsv(string $uri): void
    {
        $stream = $this->fs->writeTo(path($uri));
        $stream->append("id\n1\n");
        $stream->close();
    }
}
