<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\Parquet\Data\Converter\Int32DateTimeConverter;
use PHPUnit\Framework\TestCase;

final class Int32DateTimeConverterTest extends TestCase
{
    public function test_converting_date_times() : void
    {
        $date = new \DateTimeImmutable('2021-01-01T14:58:16.111111 UTC');

        $converter = new Int32DateTimeConverter();

        $this->assertEquals(
            new \DateTimeImmutable('2021-01-01T14:58:16.111000+0000'),
            $converter->fromParquetType($converter->toParquetType($date))
        );
    }
}
