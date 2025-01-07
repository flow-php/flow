<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, uuid_v4, uuid_v7};
use Flow\ETL\Tests\FlowTestCase;
use Ramsey\Uuid\Uuid;

final class UuidTest extends FlowTestCase
{
    protected function setUp() : void
    {
        if (!\class_exists(Uuid::class) && !\class_exists(\Symfony\Component\Uid\Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    public function test_uuid4() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $expression = uuid_v4();
        self::assertTrue(
            Uuid::isValid(
                $expression->eval(row())->toString()
            )
        );
        self::assertNotSame(
            $expression->eval(row()),
            $expression->eval(row())
        );
    }

    public function test_uuid4_is_unique() : void
    {
        $expression = uuid_v4();

        self::assertNotEquals(
            $expression->eval(row()),
            $expression->eval(row())
        );
    }

    public function test_uuid7() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        self::assertTrue(
            Uuid::isValid(
                uuid_v7(lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC'))))->eval(row())->toString()
            )
        );
    }

    public function test_uuid7_is_unique() : void
    {
        $dateTime = lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')));
        self::assertNotEquals(
            uuid_v7($dateTime)->eval(row()),
            uuid_v7($dateTime)->eval(row())
        );
    }

    public function test_uuid7_return_null_for_non_datetime_interface() : void
    {
        self::assertNull(
            uuid_v7(lit(''))->eval(row())
        );
    }
}
