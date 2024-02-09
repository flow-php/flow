<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\Parquet\Data\Converter\UuidConverter;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidConverterTest extends TestCase
{
    public function test_converting_uuid() : void
    {
        $uuid = Uuid::uuid4()->toString();

        $converter = new UuidConverter();

        $this->assertEquals(
            $uuid,
            $converter->fromParquetType($converter->toParquetType($uuid))
        );
    }
}
