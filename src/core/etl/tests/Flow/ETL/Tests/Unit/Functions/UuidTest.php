<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Uuid as UuidFunction;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function class_exists;
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
        // @mago-ignore analysis:mixed-assignment
        $result = (new FunctionContext(flow_context()))->eval($expression, [], schema());
        static::assertInstanceOf(FlowUuid::class, $result);
        static::assertTrue(Uuid::isValid($result->toString()));
        static::assertNotSame(
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
        );
    }

    public function test_uuid4_is_unique(): void
    {
        $expression = uuid_v4();

        static::assertNotEquals(
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
        );
    }

    public function test_uuid7(): void
    {
        if (!class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        // @mago-ignore analysis:mixed-assignment
        $result = (new FunctionContext(flow_context()))->eval(
            uuid_v7(lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')))),
            [],
            schema(),
        );
        static::assertInstanceOf(FlowUuid::class, $result);
        static::assertTrue(Uuid::isValid($result->toString()));
    }

    public function test_uuid7_is_unique(): void
    {
        $dateTime = lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')));
        static::assertNotEquals(
            (new FunctionContext(flow_context()))->eval(uuid_v7($dateTime), [], schema()),
            (new FunctionContext(flow_context()))->eval(uuid_v7($dateTime), [], schema()),
        );
    }

    public function test_uuid_v7_requires_a_date_time(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Uuid uuid7 function requires a DateTimeInterface value');

        (new FunctionContext(flow_context()))->eval(uuid_v7(lit('')), [], schema());
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = UuidFunction::uuid7(new DateTimeImmutable('2026-01-02 03:04:05 UTC'));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame(
            $context->columnOf($function, $rows, $schema)->type()->toString(),
            $context->columnOf($rebuilt, $rows, $schema)->type()->toString(),
        );
    }
}
