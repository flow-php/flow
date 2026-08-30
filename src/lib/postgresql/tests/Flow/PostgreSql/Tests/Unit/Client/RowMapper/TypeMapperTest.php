<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper;

use DateTimeImmutable;
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake\RecordedResult;
use Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake\SpyRowMapper;
use Flow\Types\Type;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\UuidV7;

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;

final class TypeMapperTest extends TestCase
{
    public static function provide_invalid_mappings(): Generator
    {
        yield 'map scalars' => [
            ['id' => UuidV7::generate(), 'name' => 'Alice', 'last_name' => null],
            type_structure([
                'id' => type_structure(['uuid' => type_uuid()]),
                'name' => type_string(),
                'last_name' => type_optional(type_string()),
            ]),
        ];

        yield 'jsonb is not valid json' => [
            ['metadata' => '{"theme": "dark" "notifications": true}'],
            type_structure([
                'metadata' => type_structure([
                    'theme' => type_string(),
                    'notifications' => type_boolean(),
                ]),
            ]),
        ];

        yield 'jsonb shape is missing required field' => [
            ['profile' => '{"name":"Alice"}'],
            type_structure([
                'profile' => type_structure([
                    'name' => type_string(),
                    'address' => type_structure([
                        'city' => type_string(),
                    ]),
                ]),
            ]),
        ];

        yield 'jsonb list element has wrong type' => [
            ['tags' => '["php"]'],
            type_structure([
                'tags' => type_list(type_uuid()),
            ]),
        ];
    }

    public static function provide_valid_mappings(): Generator
    {
        yield 'map scalars' => [
            ['id' => '019d92a8-54e1-70a9-bc8f-85ef98592dd4', 'name' => 'Alice', 'last_name' => null],
            type_structure([
                'id' => type_string(),
                'name' => type_string(),
                'last_name' => type_optional(type_string()),
            ]),
            ['id' => '019d92a8-54e1-70a9-bc8f-85ef98592dd4', 'name' => 'Alice', 'last_name' => null],
        ];

        yield 'map row to single value' => [
            ['id' => '019d9293-793d-7036-918a-f19abffd545c', 'name' => 'Alice', 'last_name' => null],
            type_string(),
            '{"id":"019d9293-793d-7036-918a-f19abffd545c","name":"Alice","last_name":null}',
        ];

        yield 'map to objects' => [
            ['id' => '019d9293-793d-7036-918a-f19abffd545c', 'name' => 'Alice', 'last_name' => null],
            type_structure([
                'id' => type_uuid(),
                'name' => type_string(),
                'last_name' => type_optional(type_string()),
            ]),
            [
                'id' => new Uuid('019d9293-793d-7036-918a-f19abffd545c'),
                'name' => 'Alice',
                'last_name' => null,
            ],
        ];

        yield 'map jsons to strings' => [
            ['jsonb' => '{"key": "value"}'],
            type_structure([
                'jsonb' => type_string(),
            ]),
            ['jsonb' => '{"key": "value"}'],
        ];

        yield 'map jsons to structure' => [
            ['jsonb' => '{"key": "some_key", "value": "some_value"}'],
            type_structure([
                'jsonb' => type_structure([
                    'key' => type_string(),
                    'value' => type_string(),
                ]),
            ]),
            ['jsonb' => ['key' => 'some_key', 'value' => 'some_value']],
        ];

        yield 'map jsonb to deeply nested structure' => [
            ['profile' => '{"name":"Alice","address":{"city":"Warsaw","country":"PL"}}'],
            type_structure([
                'profile' => type_structure([
                    'name' => type_string(),
                    'address' => type_structure([
                        'city' => type_string(),
                        'country' => type_string(),
                    ]),
                ]),
            ]),
            ['profile' => ['name' => 'Alice', 'address' => ['city' => 'Warsaw', 'country' => 'PL']]],
        ];

        yield 'map jsonb array to list of strings' => [
            ['tags' => '["php","postgresql","flow"]'],
            type_structure([
                'tags' => type_list(type_string()),
            ]),
            ['tags' => ['php', 'postgresql', 'flow']],
        ];

        yield 'map jsonb array to list of structures' => [
            ['addresses' => '[{"city":"Warsaw","country":"PL"},{"city":"Berlin","country":"DE"}]'],
            type_structure([
                'addresses' => type_list(type_structure([
                    'city' => type_string(),
                    'country' => type_string(),
                ])),
            ]),
            [
                'addresses' => [
                    ['city' => 'Warsaw', 'country' => 'PL'],
                    ['city' => 'Berlin', 'country' => 'DE'],
                ],
            ],
        ];

        yield 'map mixed scalar and jsonb row with datetime and uuid' => [
            [
                'id' => '019d9293-793d-7036-918a-f19abffd545c',
                'name' => 'Alice',
                'created_at' => '2024-03-15 14:30:00',
                'metadata' => '{"theme":"dark","notifications":true}',
                'age' => 42,
            ],
            type_structure([
                'id' => type_uuid(),
                'name' => type_string(),
                'created_at' => type_datetime(),
                'metadata' => type_structure([
                    'theme' => type_string(),
                    'notifications' => type_boolean(),
                ]),
                'age' => type_integer(),
            ]),
            [
                'id' => new Uuid('019d9293-793d-7036-918a-f19abffd545c'),
                'name' => 'Alice',
                'created_at' => new DateTimeImmutable('2024-03-15 14:30:00'),
                'metadata' => ['theme' => 'dark', 'notifications' => true],
                'age' => 42,
            ],
        ];

        yield 'map jsonb with optional element present' => [
            ['profile' => '{"name":"Alice","nickname":"Ali"}'],
            type_structure([
                'profile' => type_structure([
                    'name' => type_string(),
                    'nickname' => structure_element('nickname', type_string(), optional: true),
                ]),
            ]),
            ['profile' => ['name' => 'Alice', 'nickname' => 'Ali']],
        ];

        yield 'map jsonb with optional element absent' => [
            ['profile' => '{"name":"Alice"}'],
            type_structure([
                'profile' => type_structure([
                    'name' => type_string(),
                    'nickname' => structure_element('nickname', type_string(), optional: true),
                ]),
            ]),
            ['profile' => ['name' => 'Alice']],
        ];
    }

    public function test_chaining_forwards_jsonb_decoded_payload_to_next(): void
    {
        $spy = new SpyRowMapper();

        $result = type_instance_of(RecordedResult::class)->assert(type_mapper(type_structure([
            'metadata' => type_structure([
                'theme' => type_string(),
            ]),
        ]), $spy)->map(['metadata' => '{"theme":"dark"}'], MapperContextMother::any()));

        static::assertSame(['metadata' => ['theme' => 'dark']], $result->row);
    }

    public function test_chaining_forwards_same_context_to_next(): void
    {
        $spy = new SpyRowMapper();
        $context = MapperContextMother::any();

        type_mapper(type_structure([
            'metadata' => type_structure([
                'theme' => type_string(),
            ]),
        ]), $spy)->map(['metadata' => '{"theme":"dark"}'], $context);

        static::assertCount(1, $spy->receivedContexts);
        static::assertSame($context, $spy->receivedContexts[0]);
    }

    public function test_chaining_is_skipped_when_cast_fails(): void
    {
        $spy = new SpyRowMapper();

        try {
            type_mapper(type_structure([
                'metadata' => type_structure([
                    'theme' => type_string(),
                ]),
            ]), $spy)->map(['metadata' => '{not valid json}'], MapperContextMother::any());
            static::fail('Expected MappingException was not thrown');
        } catch (MappingException) {
            static::assertSame([], $spy->receivedRows);
        }
    }

    /**
     * @param array<string, mixed> $data
     * @param Type<mixed> $type
     */
    #[DataProvider('provide_invalid_mappings')]
    public function test_invalid_mapping(array $data, Type $type): void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Failed to map database row to type:');
        type_mapper($type)->map($data, MapperContextMother::any());
    }

    /**
     * @param array<string, mixed> $data
     * @param Type<mixed> $type
     */
    #[DataProvider('provide_valid_mappings')]
    public function test_valid_mapping(array $data, Type $type, mixed $output): void
    {
        static::assertEquals($output, type_mapper($type)->map($data, MapperContextMother::any()));
    }

    public function test_when_next_is_null_returns_cast_result_directly(): void
    {
        static::assertSame(
            ['id' => 'abc', 'name' => 'Alice'],
            type_mapper(type_structure([
                'id' => type_string(),
                'name' => type_string(),
            ]))->map(['id' => 'abc', 'name' => 'Alice'], MapperContextMother::any()),
        );
    }

    public function test_when_next_is_provided_cast_result_is_forwarded_to_next(): void
    {
        $spy = new SpyRowMapper();

        $result = type_instance_of(RecordedResult::class)->assert(type_mapper(type_structure([
            'id' => type_string(),
            'name' => type_string(),
        ]), $spy)->map(['id' => 'abc', 'name' => 'Alice'], MapperContextMother::any()));

        static::assertSame(['id' => 'abc', 'name' => 'Alice'], $result->row);
        static::assertSame([['id' => 'abc', 'name' => 'Alice']], $spy->receivedRows);
    }
}
