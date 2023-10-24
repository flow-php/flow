<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\Parquet\Data\Converter\Int32DateConverter;
use PHPUnit\Framework\TestCase;

final class Int32DateConverterTest extends TestCase
{
    public function test_converting_dates() : void
    {
        $date = new \DateTimeImmutable('2021-01-01 00:00:00 UTC');

        $converter = new Int32DateConverter();

        $this->assertEquals(
            $date,
            $converter->fromParquetType($converter->toParquetType($date))
        );
    }
}
