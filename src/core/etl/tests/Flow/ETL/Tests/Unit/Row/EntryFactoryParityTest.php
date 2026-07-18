<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid as FlowUuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class EntryFactoryParityTest extends FlowTestCase
{
    public static function provide_native_values(): Generator
    {
        yield 'integer' => [1, type_integer()];
        yield 'string' => ['flow', type_string()];
        yield 'datetime' => [new DateTimeImmutable('2024-04-01 10:00:00 UTC'), type_datetime()];
        yield 'time' => [new DateInterval('PT1H'), type_time()];
        yield 'uuid' => [new FlowUuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'), type_uuid()];
        yield 'json object' => [new Json('{"id":1}'), type_json()];
        yield 'json array' => [new Json('[1,2,3]'), type_json()];
        yield 'list of int' => [[1, 2, 3], type_list(type_integer())];
        yield 'map' => [['a' => 1, 'b' => 2], type_map(type_string(), type_integer())];
        yield 'structure' => [
            ['id' => 1, 'name' => 'flow'],
            type_structure(['id' => type_integer(), 'name' => type_string()]),
        ];
        yield 'nested structure' => [
            ['geo' => ['lat' => 1, 'lon' => 2], 'name' => 'flow'],
            type_structure([
                'geo' => type_map(type_string(), type_integer()),
                'name' => type_string(),
            ]),
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('provide_native_values')]
    public function test_trusted_create_matches_casting_coerce(mixed $value, Type $type): void
    {
        $factory = new EntryFactory();

        static::assertEquals($factory->cast('e', $value, $type), $factory->create('e', $value, $type));
    }

    public function test_trusted_create_fails_loudly_for_a_value_that_does_not_match_the_type(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new EntryFactory())->create('e', '2024-01-01 10:00:00 UTC', type_datetime());
    }
}
