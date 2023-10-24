<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\Parquet\Data\Converter\Int64DateTimeConverter;
use PHPUnit\Framework\TestCase;

final class Int64DateTimeConverterTest extends TestCase
{
    public function test_converting_date_times() : void
    {
        $date = (new \DateTimeImmutable('2021-01-01'))
            ->setTimezone(new \DateTimeZone('UTC'))
            ->setTime(14, 56, 24, 54324);

        $converter = new Int64DateTimeConverter();

        $this->assertEquals(
            $date,
            $converter->fromParquetType($converter->toParquetType($date))
        );
    }
}
