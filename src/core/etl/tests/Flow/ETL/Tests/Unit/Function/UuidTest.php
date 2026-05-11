<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;

final class UuidTest extends FlowTestCase
{
    protected function setUp(): void
    {
        if (!\class_exists(Uuid::class) && !\class_exists(\Symfony\Component\Uid\Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    public function test_uuid4(): void
    {
        if (!\class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $expression = uuid_v4();
        $result = $expression->eval(row(), flow_context());
        static::assertInstanceOf(FlowUuid::class, $result->value);
        static::assertTrue(Uuid::isValid($result->value->toString()));
        static::assertNotSame($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_uuid4_is_unique(): void
    {
        $expression = uuid_v4();

        static::assertNotEquals($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_uuid7(): void
    {
        if (!\class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $result = uuid_v7(lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC'))))
            ->eval(row(), flow_context());
        static::assertInstanceOf(FlowUuid::class, $result->value);
        static::assertTrue(Uuid::isValid($result->value->toString()));
    }

    public function test_uuid7_is_unique(): void
    {
        $dateTime = lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')));
        static::assertNotEquals(
            uuid_v7($dateTime)->eval(row(), flow_context()),
            uuid_v7($dateTime)->eval(row(), flow_context()),
        );
    }

    public function test_uuid7_return_null_for_non_datetime_interface(): void
    {
        static::assertNull(uuid_v7(lit(''))->eval(row(), flow_context())->value);
    }
}
