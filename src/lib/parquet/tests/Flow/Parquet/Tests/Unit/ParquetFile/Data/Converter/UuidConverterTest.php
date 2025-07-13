<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidConverterTest extends TestCase
{
    public function test_converting_uuid() : void
    {
        $uuid = Uuid::uuid4()->toString();

        $converter = new UuidConverter();

        self::assertEquals(
            $uuid,
            $converter->fromParquetType($converter->toParquetType($uuid))
        );
    }
}
