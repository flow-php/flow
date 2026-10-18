<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Index;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class IndexLocationTest extends TestCase
{
    #[TestWith([-1, 10])]
    #[TestWith([0, 0])]
    public function test_a_location_before_the_file_or_of_no_bytes_is_refused(int $offset, int $length): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'An index location needs an offset of at least 0 and a length of at least 1, got '
            . $offset
            . ' and '
            . $length,
        );

        new IndexLocation($offset, $length);
    }

    #[TestWith([null, 10])]
    #[TestWith([10, null])]
    #[TestWith([-1, 10])]
    #[TestWith([10, 0])]
    public function test_an_absent_or_unreadable_thrift_location_is_none(?int $offset, ?int $length): void
    {
        static::assertNull(IndexLocation::fromThrift($offset, $length));
    }

    public function test_a_thrift_location_is_read_as_ints(): void
    {
        static::assertEquals(new IndexLocation(4, 20), IndexLocation::fromThrift(4, 20));
    }
}
