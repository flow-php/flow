<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\ListedFiles;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class ListedFilesTest extends FlowTestCase
{
    public function test_a_zero_byte_member_reported_as_null_makes_the_total_unknown(): void
    {
        $empty = native_local_filesystem()->status(path(__DIR__ . '/../../Fixtures/empty.txt'));

        static::assertNotNull($empty);
        static::assertEquals(
            Cardinality::unknown(),
            ListedFiles::of([
                new SourceFile(path('memory://orders/a.csv'), 100),
                new SourceFile($empty->path, $empty->size),
            ])->bytes,
        );
    }

    public function test_an_empty_listing_is_zero_bytes_exactly(): void
    {
        static::assertEquals(Cardinality::exact(0), ListedFiles::of([])->bytes);
    }

    public function test_it_sums_the_listed_sizes(): void
    {
        static::assertEquals(
            Cardinality::exact(300),
            ListedFiles::of([
                new SourceFile(path('memory://orders/a.csv'), 100),
                new SourceFile(path('memory://orders/b.csv'), 200),
            ])->bytes,
        );
    }

    public function test_one_member_without_a_size_makes_the_total_unknown(): void
    {
        static::assertEquals(
            Cardinality::unknown(),
            ListedFiles::of([
                new SourceFile(path('memory://orders/a.csv')),
                new SourceFile(path('memory://orders/b.csv'), 200),
            ])->bytes,
        );
    }

    public function test_it_counts_every_listed_file_sized_or_not(): void
    {
        static::assertSame(
            2,
            ListedFiles::of([
                new SourceFile(path('memory://orders/a.csv')),
                new SourceFile(path('memory://orders/b.csv'), 200),
            ])->count,
        );
    }

    public function test_it_reads_file_statuses_as_well(): void
    {
        $listed = ListedFiles::of([new FileStatus(path('memory://orders/a.csv'), true, 100)]);

        static::assertSame(1, $listed->count);
        static::assertEquals(Cardinality::exact(100), $listed->bytes);
    }

    public function test_a_negative_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Listed file count must not be negative, given: -1');

        new ListedFiles(-1, Cardinality::unknown());
    }
}
