<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\Parquet\Data\Converter\TimeConverter;

final class TimeConverterTest extends TestCase
{
    public function test_converting_diff_between_two_datetimes_to_time() : void
    {
        $start = (new \DateTimeImmutable('2023-01-01 00:00:00 UTC'));
        $end = (new \DateTimeImmutable('2023-01-01 00:00:0 UTC'))->setTime(15, 25, 30, 15040);

        $diff = $start->diff($end);

        $microseconds = (new TimeConverter())->toParquetType($diff);

        $this->assertEquals(
            $diff,
            (new TimeConverter())->fromParquetType($microseconds)
        );
    }
}
