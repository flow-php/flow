<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class SameTest extends FlowTestCase
{
    public function test_null_is_identical_to_null(): void
    {
        // PHP identity, not SQL equality - ->same() is the migration path for both-null checks.
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->same(lit(null)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->same(lit(1)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
    }

    /**
     * @return Generator<string, array{Type<mixed>, mixed, mixed, bool}>
     */
    public static function values_of_one_type(): Generator
    {
        yield 'uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000001',
            true,
        ];
        yield 'other uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000002',
            false,
        ];
        yield 'json' => [type_json(), '{"a":1,"b":2}', '{"a":1,"b":2}', true];
        yield 'json compares its stored text' => [type_json(), '{"a":1,"b":2}', '{"b":2,"a":1}', false];
        yield 'enum' => [type_enum(BackedStringEnum::class), BackedStringEnum::one, BackedStringEnum::one, true];
        yield 'datetime of the same instant in another zone' => [
            type_datetime(),
            new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')),
            true,
        ];
        yield 'date' => [type_date(), new DateTimeImmutable('2024-01-01'), new DateTimeImmutable('2024-01-01'), true];
        yield 'time' => [type_time(), new DateInterval('PT60M'), new DateInterval('PT1H'), true];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_of_one_type')]
    public function test_values_of_one_type_are_the_same_by_value(
        Type $type,
        mixed $left,
        mixed $right,
        bool $same,
    ): void {
        static::assertSame($same, (new FunctionContext(flow_context()))->eval(
            ref('a')->same(ref('b')),
            ['a' => $left, 'b' => $right],
            schema(definition_from_type('a', $type), definition_from_type('b', $type)),
        ));
    }
}
