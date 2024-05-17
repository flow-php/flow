<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_int, type_uuid};
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_uuid()->isEqual(type_uuid())
        );
        self::assertFalse(
            type_uuid()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_uuid(true)->isValid(null));
        self::assertFalse(type_uuid()->isValid('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'));
        self::assertFalse(type_uuid()->isValid('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5'));
        self::assertFalse(type_uuid()->isValid('2'));
        self::assertTrue(type_uuid()->isValid(Uuid::uuid4()));
        self::assertTrue(type_uuid()->isValid(\Symfony\Component\Uid\Uuid::v4()));
        self::assertTrue(type_uuid()->isValid(new \Flow\ETL\PHP\Value\Uuid(Uuid::uuid4())));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'uuid',
            type_uuid()->toString()
        );
        self::assertSame(
            '?uuid',
            type_uuid(true)->toString()
        );
    }
}
