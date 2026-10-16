<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use Flow\Parquet\Tests\Double\StringableUuid;
use Flow\Parquet\Tests\Double\ToStringUuid;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

use function hex2bin;

final class UuidConverterTest extends TestCase
{
    public function test_converting_uuid(): void
    {
        $uuid = Uuid::uuid4()->toString();

        $converter = new UuidConverter();

        static::assertEquals($uuid, $converter->fromParquetType($converter->toParquetType($uuid)));
    }

    public function test_physical_value_is_the_16_raw_bytes_whatever_the_case(): void
    {
        $converter = new UuidConverter();
        $bytes = (string) hex2bin('550e8400e29b41d4a716446655440000');

        static::assertSame($bytes, $converter->toParquetType('550E8400-E29B-41D4-A716-446655440000'));
        static::assertSame($bytes, $converter->toParquetType(new ToStringUuid('550e8400-e29b-41d4-a716-446655440000')));
        static::assertSame(
            $bytes,
            $converter->toParquetType(new StringableUuid('550e8400-e29b-41d4-a716-446655440000')),
        );
        static::assertSame('550e8400-e29b-41d4-a716-446655440000', $converter->fromParquetType($bytes));
    }

    #[TestWith(['550e8400-e29b-41d4-a716-44665544000'])]
    #[TestWith(['zz0e8400-e29b-41d4-a716-446655440000'])]
    public function test_malformed_uuid_is_refused(string $uuid): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Invalid UUID format: ' . $uuid);

        (new UuidConverter())->toParquetType($uuid);
    }

    public function test_reading_anything_but_16_bytes_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('UUID must be read as 16 bytes from Parquet file');

        (new UuidConverter())->fromParquetType('550e8400-e29b-41d4-a716-446655440000');
    }

    public function test_writing_a_non_uuid_value_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('UUID must be written as a string or Stringable object');

        (new UuidConverter())->toParquetType(1);
    }
}
