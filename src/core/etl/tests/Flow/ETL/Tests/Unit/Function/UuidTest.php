<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, lit, uuid_v4, uuid_v7};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid as FlowUuid;
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
        $result = $expression->eval(row(), flow_context());
        self::assertInstanceOf(FlowUuid::class, $result->value);
        self::assertTrue(
            Uuid::isValid(
                $result->value->toString()
            )
        );
        self::assertNotSame(
            $expression->eval(row(), flow_context()),
            $expression->eval(row(), flow_context())
        );
    }

    public function test_uuid4_is_unique() : void
    {
        $expression = uuid_v4();

        self::assertNotEquals(
            $expression->eval(row(), flow_context()),
            $expression->eval(row(), flow_context())
        );
    }

    public function test_uuid7() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $result = uuid_v7(lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC'))))->eval(row(), flow_context());
        self::assertInstanceOf(FlowUuid::class, $result->value);
        self::assertTrue(
            Uuid::isValid(
                $result->value->toString()
            )
        );
    }

    public function test_uuid7_is_unique() : void
    {
        $dateTime = lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')));
        self::assertNotEquals(
            uuid_v7($dateTime)->eval(row(), flow_context()),
            uuid_v7($dateTime)->eval(row(), flow_context())
        );
    }

    public function test_uuid7_return_null_for_non_datetime_interface() : void
    {
        self::assertNull(
            uuid_v7(lit(''))->eval(row(), flow_context())->value
        );
    }
}
