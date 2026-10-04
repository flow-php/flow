<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function hex2bin;
use function str_repeat;

final class UuidTest extends TestCase
{
    public function test_from_bytes_builds_canonical_lowercase_text(): void
    {
        $uuid = Uuid::fromBytes((string) hex2bin('6C2F1D4E8B3A4C5D9E6F0A1B2C3D4E5F'));

        static::assertSame('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f', $uuid->toString());
        static::assertEquals(new Uuid('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f'), $uuid);
    }

    public function test_from_bytes_refuses_fifteen_bytes(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Uuid::fromBytes() expects 16 bytes, got 15');

        Uuid::fromBytes(str_repeat('a', 15));
    }

    public function test_from_bytes_refuses_seventeen_bytes(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Uuid::fromBytes() expects 16 bytes, got 17');

        Uuid::fromBytes(str_repeat('a', 17));
    }

    public function test_construct_with_invalid_string_uuid_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new Uuid('invalid-uuid-string');
    }

    public function test_construct_with_invalid_uuid_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new Uuid('********-****-****-****************');
    }

    public function test_construct_with_ramsey_uuid_instance(): void
    {
        $ramseyUuid = RamseyUuid::uuid4();
        $uuid = new Uuid($ramseyUuid);

        static::assertSame($ramseyUuid->toString(), $uuid->toString());
    }

    public function test_construct_with_symfony_uuid_instance(): void
    {
        $symfonyUuid = SymfonyUuid::v4();
        $uuid = new Uuid($symfonyUuid);

        static::assertSame($symfonyUuid->toRfc4122(), $uuid->toString());
    }

    public function test_construct_with_valid_string_uuid(): void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = new Uuid($uuidString);

        static::assertSame($uuidString, $uuid->toString());
    }

    public function test_from_string_creates_instance(): void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = Uuid::fromString($uuidString);

        static::assertSame($uuidString, $uuid->toString());
    }

    public function test_is_equal_with_different_uuid(): void
    {
        $uuid1 = new Uuid('123e4567-e89b-12d3-a456-426614174000');
        $uuid2 = new Uuid('123e4567-e89b-12d3-a456-426614174001');

        static::assertFalse($uuid1->isEqual($uuid2));
    }

    public function test_is_equal_with_same_uuid(): void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid1 = new Uuid($uuidString);
        $uuid2 = new Uuid($uuidString);

        static::assertTrue($uuid1->isEqual($uuid2));
    }

    public function test_to_string_returns_correct_value(): void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = new Uuid($uuidString);

        static::assertSame($uuidString, (string) $uuid);
    }
}
