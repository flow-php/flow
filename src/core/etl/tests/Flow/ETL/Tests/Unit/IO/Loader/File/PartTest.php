<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Loader\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\partition_by;
use function Flow\Filesystem\DSL\path;
use function max;

final class PartTest extends FlowTestCase
{
    #[TestWith([0, 1, false, '00000'])]
    #[TestWith([3, 160, false, '00003'])]
    #[TestWith([12345, 200000, false, '012345'])]
    #[TestWith([3, 160, true, '00003-1a2b3c4d5e6f7a8b'])]
    public function test_name(int $index, int $splits, bool $append, string $expected): void
    {
        static::assertSame($expected, (new Part(max(0, $index), max(1, $splits), '1a2b3c4d5e6f7a8b', $append))->name());
    }

    public function test_normalize_round_trips(): void
    {
        $part = new Part(3, 160, '1a2b3c4d5e6f7a8b', true);

        static::assertSame(
            ['index' => 3, 'splits' => 160, 'run' => '1a2b3c4d5e6f7a8b', 'append' => true],
            $part->normalize(),
        );
        static::assertEquals($part, Part::fromArray($part->normalize()));
    }

    public function test_fill_replaces_the_placeholder_in_the_basename(): void
    {
        static::assertSame(
            'file:///out/orders.parquet/part-00003.parquet',
            (new Part(3, 10, 'abcd', false))->fill(path('file:///out/orders.parquet/part-{part}.parquet'))->uri(),
        );
        static::assertSame(
            'file:///out/orders-00000-abcd.csv',
            (new Part(0, 1, 'abcd', true))->fill(path('file:///out/orders-{part}.csv'))->uri(),
        );
    }

    public function test_fill_without_placeholder_is_identity(): void
    {
        $destination = path('file:///out/orders.csv');

        static::assertSame($destination, (new Part(3, 10, 'abcd', false))->fill($destination));
    }

    public function test_a_placeholder_in_a_directory_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('names a file, not a directory');

        (new Part(3, 10, 'abcd', false))->fill(path('file:///out/{part}/orders.csv'));
    }

    public function test_in(): void
    {
        static::assertTrue(Part::in(path('file:///out/part-{part}.csv')));
        static::assertFalse(Part::in(path('file:///out/part-{dept}.csv')));
    }

    public function test_glob(): void
    {
        static::assertSame('file:///out/part-*.csv', Part::glob(path('file:///out/part-{part}.csv'), false)->uri());
        static::assertSame('file:///out/**/part-*.csv', Part::glob(path('file:///out/part-{part}.csv'), true)->uri());
        static::assertSame(
            'file:///out/*/**/part-*.csv',
            Part::glob(path('file:///out/{dept}/part-{part}.csv'), true)->uri(),
        );
    }

    public function test_refuse_reserved_throws_for_a_part_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot name the partition column "part"');

        Part::refuseReserved(path('file:///out/{part}/orders-{part}.csv'), partition_by('part'));
    }

    public function test_refuse_reserved_allows_other_columns_and_plain_destinations(): void
    {
        $this->expectNotToPerformAssertions();

        Part::refuseReserved(path('file:///out/orders-{part}.csv'), partition_by('dept'));
        Part::refuseReserved(path('file:///out/orders.csv'), partition_by('part'));
        Part::refuseReserved(path('file:///out/orders-{part}.csv'), Partitioning::none());
    }
}
