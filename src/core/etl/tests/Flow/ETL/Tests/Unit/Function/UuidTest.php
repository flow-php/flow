<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function class_exists;
use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;

final class UuidTest extends FlowTestCase
{
    protected function setUp(): void
    {
        if (!class_exists(Uuid::class) && !class_exists(SymfonyUuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    public function test_uuid4(): void
    {
        if (!class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $expression = uuid_v4();
        $result = $expression->eval(array_to_row([], schema()), flow_context());
        static::assertInstanceOf(FlowUuid::class, $result);
        static::assertTrue(Uuid::isValid($result->toString()));
        static::assertNotSame(
            $expression->eval(array_to_row([], schema()), flow_context()),
            $expression->eval(array_to_row([], schema()), flow_context()),
        );
    }

    public function test_uuid4_is_unique(): void
    {
        $expression = uuid_v4();

        static::assertNotEquals(
            $expression->eval(array_to_row([], schema()), flow_context()),
            $expression->eval(array_to_row([], schema()), flow_context()),
        );
    }

    public function test_uuid7(): void
    {
        if (!class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $result = uuid_v7(lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC'))))
            ->eval(array_to_row([], schema()), flow_context());
        static::assertInstanceOf(FlowUuid::class, $result);
        static::assertTrue(Uuid::isValid($result->toString()));
    }

    public function test_uuid7_is_unique(): void
    {
        $dateTime = lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')));
        static::assertNotEquals(
            uuid_v7($dateTime)->eval(array_to_row([], schema()), flow_context()),
            uuid_v7($dateTime)->eval(array_to_row([], schema()), flow_context()),
        );
    }

    public function test_uuid_v7_requires_a_date_time(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Uuid uuid7 function requires a DateTimeInterface value');

        uuid_v7(lit(''))->eval(array_to_row([], schema()), flow_context());
    }
}
