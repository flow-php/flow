<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\ByteUnit;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\RowRange;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Split;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SourceStream;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function array_sum;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FileSplitTest extends FlowTestCase
{
    public function test_a_negative_start_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('File split start must not be negative, given: -1');

        new FileSplit(path('memory://orders.csv'), -1, null);
    }

    public function test_a_null_length_means_to_the_end_of_the_file(): void
    {
        static::assertNull((new FileSplit(path('memory://orders.csv'), 0, null))->length);
    }

    public function test_a_zero_length_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('File split length must be greater than 0, given: 0');

        new FileSplit(path('memory://orders.csv'), 0, 0);
    }

    public function test_it_accepts_the_smallest_valid_values(): void
    {
        $split = new FileSplit(path('memory://orders.csv'), 0, 1);

        static::assertSame(0, $split->start);
        static::assertSame(1, $split->length);
        static::assertTrue($split->isFirst());
    }

    public function test_only_the_window_at_the_start_of_the_file_is_first(): void
    {
        static::assertTrue((new FileSplit(path('memory://orders.csv'), 0, null))->isFirst());
        static::assertFalse((new FileSplit(path('memory://orders.csv'), 1, null))->isFirst());
    }

    public function test_the_bytes_a_launcher_sizes_by_are_the_window_length(): void
    {
        static::assertSame(8_388_608, (new FileSplit(path('memory://orders.csv'), 0, 8_388_608, 20_000_000))->bytes());
    }

    public function test_the_bytes_of_a_whole_file_split_are_the_listed_file_size(): void
    {
        static::assertSame(20_000_000, (new FileSplit(path('memory://orders.csv'), 0, null, 20_000_000))->bytes());
    }

    public function test_the_bytes_of_an_unlisted_whole_file_split_are_unknown(): void
    {
        static::assertSame(0, (new FileSplit(path('memory://orders.csv'), 0, null))->bytes());
    }

    public function test_it_is_a_window_of_one_file(): void
    {
        $split = new FileSplit(path('memory://orders.csv'), 25_165_824, 8_388_608);

        static::assertInstanceOf(Split::class, $split);
        static::assertSame('memory://orders.csv', $split->path->uri());
        static::assertSame(25_165_824, $split->start);
        static::assertSame(8_388_608, $split->length);
        static::assertFalse($split->isFirst());
    }

    public function test_a_negative_size_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('File split size must not be negative, given: -1');

        new FileSplit(path('memory://orders.csv'), 0, null, -1);
    }

    public function test_a_split_describes_itself_and_is_rebuilt_equal(): void
    {
        $split = new FileSplit(path('memory://orders.csv'), 10, 5, 100);

        static::assertSame(
            [
                'path' => ['uri' => 'memory://orders.csv', 'options' => [], 'partitions' => []],
                'start' => 10,
                'length' => 5,
                'size' => 100,
            ],
            $split->describe()->data,
        );
        static::assertFalse($split->describe()->sensitive());
        static::assertEquals($split, (new Rebuild())->object(
            Description::fromArray($split->describe()->normalize()),
            Split::class,
        ));
    }

    public function test_an_unsized_window_to_the_end_rebuilds_with_nulls(): void
    {
        $split = new FileSplit(path('memory://orders.csv'), 0, null);

        static::assertEquals($split, (new Rebuild())->object($split->describe(), FileSplit::class));
    }

    public function test_a_file_smaller_than_the_target_is_one_window(): void
    {
        static::assertEquals(
            [new FileSplit(path('memory://orders.csv'), 0, 100, 100)],
            FileSplit::windows(new SourceFile(path('memory://orders.csv'), 100), 8_388_608),
        );
    }

    #[TestWith([null])]
    #[TestWith([0])]
    public function test_an_unknown_size_is_one_whole_file_split(?int $size): void
    {
        static::assertEquals(
            [new FileSplit(path('memory://orders.csv'), 0, null, $size)],
            FileSplit::windows(new SourceFile(path('memory://orders.csv'), $size), 1),
        );
    }

    public function test_windows_cover_the_whole_file_and_the_last_one_is_shorter(): void
    {
        $windows = FileSplit::windows(new SourceFile(path('memory://orders.csv'), 1_000), 300);

        static::assertSame([0, 300, 600, 900], array_map(static fn(FileSplit $split): int => $split->start, $windows));
        static::assertSame(
            1_000,
            array_sum(array_map(static fn(FileSplit $split): int => $split->length ?? 0, $windows)),
        );
        static::assertEquals(new FileSplit(path('memory://orders.csv'), 900, 100, 1_000), $windows[3]);
    }

    public function test_only_the_first_window_is_first(): void
    {
        static::assertSame(
            [true, false, false],
            array_map(
                static fn(FileSplit $split): bool => $split->isFirst(),
                FileSplit::windows(new SourceFile(path('memory://orders.csv'), 3), 1),
            ),
        );
    }

    public function test_a_split_without_a_length_owns_every_unit(): void
    {
        static::assertEquals(
            new RowRange(0, 30),
            (new FileSplit(path('memory://a.parquet'), 0, null))->owns([
                new ByteUnit(4, 100, 10),
                new ByteUnit(104, 100, 20),
            ]),
        );
    }

    public function test_a_unit_belongs_to_the_window_holding_its_midpoint(): void
    {
        $units = [new ByteUnit(4, 100, 10), new ByteUnit(104, 100, 20), new ByteUnit(204, 100, 30)];

        static::assertEquals(
            [new RowRange(0, 10), new RowRange(10, 20), new RowRange(30, 30), new RowRange(60, 0)],
            array_map(
                static fn(FileSplit $split): RowRange => $split->owns($units),
                FileSplit::windows(new SourceFile(path('memory://a.parquet'), 400), 100),
            ),
        );
    }

    public function test_a_window_holding_no_midpoint_owns_nothing(): void
    {
        static::assertEquals(
            new RowRange(10, 0),
            (new FileSplit(path('memory://a.parquet'), 100, 10))->owns([
                new ByteUnit(4, 100, 10),
                new ByteUnit(104, 100, 20),
            ]),
        );
    }

    /**
     * @param int<1, max> $target
     */
    #[TestWith([1])]
    #[TestWith([7])]
    #[TestWith([50])]
    #[TestWith([1_000])]
    public function test_windows_of_any_size_own_every_row_exactly_once(int $target): void
    {
        $units = [
            new ByteUnit(4, 100, 10),
            new ByteUnit(104, 37, 20),
            new ByteUnit(141, 0, 5),
            new ByteUnit(141, 90, 30),
        ];
        $owned = array_map(
            static fn(FileSplit $split): RowRange => $split->owns($units),
            FileSplit::windows(new SourceFile(path('memory://a.parquet'), 235), $target),
        );

        $next = 0;

        foreach ($owned as $range) {
            if ($range->count === 0) {
                continue;
            }

            static::assertSame($next, $range->offset);
            $next += $range->count;
        }

        static::assertSame(65, $next);
    }

    public function test_the_first_window_starts_at_zero_and_ends_at_the_end_boundary(): void
    {
        $stream = self::content('0123456789');

        static::assertSame(
            '01234',
            (new FileSplit(path('memory://f.txt'), 0, 4))
                ->window($stream, 2, 5)
                ->content(),
        );
    }

    public function test_a_later_window_starts_at_its_start_boundary(): void
    {
        static::assertSame(
            '23456789',
            (new FileSplit(path('memory://f.txt'), 1, null))
                ->window(self::content('0123456789'), 2, null)
                ->content(),
        );
    }

    public function test_a_window_without_a_start_boundary_is_empty(): void
    {
        static::assertSame(
            '',
            (new FileSplit(path('memory://f.txt'), 3, 2))
                ->window(self::content('0123456789'), null, 7)
                ->content(),
        );
    }

    public static function content(string $content): SourceStream
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://f.txt'))->append($content)->close();

        return $filesystem->readFrom(path('memory://f.txt'));
    }
}
