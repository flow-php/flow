<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Extractor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function array_column;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class FileSourceFilesystemTest extends FlowIntegrationTestCase
{
    /**
     * The listing sources guard their (Filesystem, Path) pair at construction. Every file format's own source and sink
     * run the same case through FileFormatContract in their package.
     *
     * @param array{string, string} $dsl namespace and function name of the DSL constructor
     */
    #[TestWith([['Flow\ETL\DSL', 'files']])]
    #[TestWith([['Flow\ETL\DSL', 'from_path_partitions']])]
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

    public function test_floe_opens_a_listed_file_once_for_its_footer_and_once_for_its_rows(): void
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
        // the plan bind reads the footer through its own handle before any row flows; measured
        // 2026-09-07 as +1 open on floe and parquet only - csv and json are unchanged
        static::assertSame(2, $counting->readFromCalls, 'the footer read and the row read open once each');
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

    public function test_a_shared_memory_filesystem_round_trips_but_two_calls_do_not(): void
    {
        // two memory_filesystem() calls are two separate stores; only a shared instance round-trips
        $shared = memory_filesystem();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_floe(path('memory://s1.floe'), filesystem: $shared))
            ->run();

        $paired = [];
        df()
            ->read(from_floe(path('memory://s1.floe'), filesystem: $shared))
            ->write(to_array($paired))
            ->run();

        static::assertSame([1, 2], array_column($paired, 'id'));

        $unpaired = [];
        df()
            ->read(from_floe(path('memory://s1.floe'), filesystem: memory_filesystem())->withSchema(schema(int_schema(
                'id',
            ))))
            ->write(to_array($unpaired))
            ->run();

        static::assertSame([], $unpaired);
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
