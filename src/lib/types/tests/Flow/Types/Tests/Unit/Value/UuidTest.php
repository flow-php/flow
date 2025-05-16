<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Value;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

final class UuidTest extends TestCase
{
    public function test_construct_with_invalid_string_uuid_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        new Uuid('invalid-uuid-string');
    }

    public function test_construct_with_ramsey_uuid_instance() : void
    {
        $ramseyUuid = RamseyUuid::uuid4();
        $uuid = new Uuid($ramseyUuid);

        self::assertSame($ramseyUuid->toString(), $uuid->toString());
    }

    public function test_construct_with_symfony_uuid_instance() : void
    {
        $symfonyUuid = SymfonyUuid::v4();
        $uuid = new Uuid($symfonyUuid);

        self::assertSame($symfonyUuid->toRfc4122(), $uuid->toString());
    }

    public function test_construct_with_valid_string_uuid() : void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = new Uuid($uuidString);

        self::assertSame($uuidString, $uuid->toString());
    }

    public function test_from_string_creates_instance() : void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = Uuid::fromString($uuidString);

        self::assertInstanceOf(Uuid::class, $uuid);
        self::assertSame($uuidString, $uuid->toString());
    }

    public function test_is_equal_with_different_uuid() : void
    {
        $uuid1 = new Uuid('123e4567-e89b-12d3-a456-426614174000');
        $uuid2 = new Uuid('123e4567-e89b-12d3-a456-426614174001');

        self::assertFalse($uuid1->isEqual($uuid2));
    }

    public function test_is_equal_with_same_uuid() : void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid1 = new Uuid($uuidString);
        $uuid2 = new Uuid($uuidString);

        self::assertTrue($uuid1->isEqual($uuid2));
    }

    public function test_to_string_returns_correct_value() : void
    {
        $uuidString = '123e4567-e89b-12d3-a456-426614174000';
        $uuid = new Uuid($uuidString);

        self::assertSame($uuidString, (string) $uuid);
    }
}
