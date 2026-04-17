<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit;

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_datetime, type_integer, type_string, type_structure};
use CuyZ\Valinor\Mapper\MappingError;
use CuyZ\Valinor\MapperBuilder;
use Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture\{Address, NullableDto, SimpleDto, UserWithAddress, WithDateTime, WithTags};
use Flow\PostgreSql\Bridge\Valinor\ValinorTreeMapper;
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use PHPUnit\Framework\TestCase;

final class ValinorTreeMapperTest extends TestCase
{
    public function test_allows_extra_columns_when_superfluous_keys_enabled() : void
    {
        $mapper = new ValinorTreeMapper(
            (new MapperBuilder())->allowSuperfluousKeys()->mapper(),
            SimpleDto::class,
        );

        $result = $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'email' => 'jane@example.com',
            'extra' => 'ignored',
        ], MapperContextMother::any());

        self::assertSame(1, $result->id);
        self::assertSame('Jane', $result->name);
        self::assertSame('jane@example.com', $result->email);
    }

    public function test_composes_with_type_mapper_for_datetime_columns() : void
    {
        $mapper = type_mapper(
            type_structure([
                'id' => type_integer(),
                'createdAt' => type_datetime(),
            ]),
            new ValinorTreeMapper((new MapperBuilder())->mapper(), WithDateTime::class),
        );

        $result = $mapper->map([
            'id' => 7,
            'createdAt' => '2024-03-15 14:30:00',
        ], MapperContextMother::any());

        self::assertInstanceOf(WithDateTime::class, $result);
        self::assertSame(7, $result->id);
        self::assertSame('2024-03-15 14:30:00', $result->createdAt->format('Y-m-d H:i:s'));
    }

    public function test_composes_with_type_mapper_to_decode_jsonb_into_nested_object() : void
    {
        $mapper = type_mapper(
            type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ]),
            new ValinorTreeMapper((new MapperBuilder())->mapper(), UserWithAddress::class),
        );

        $result = $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'address' => '{"street":"Main 1","city":"Warsaw"}',
        ], MapperContextMother::any());

        self::assertInstanceOf(UserWithAddress::class, $result);
        self::assertInstanceOf(Address::class, $result->address);
        self::assertSame('Main 1', $result->address->street);
        self::assertSame('Warsaw', $result->address->city);
    }

    public function test_exception_message_includes_target_class() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), SimpleDto::class);

        try {
            $mapper->map(['id' => 1], MapperContextMother::any());
            self::fail('Expected MappingException was not thrown');
        } catch (MappingException $e) {
            self::assertStringStartsWith(
                \sprintf('Failed to map row to "%s":', SimpleDto::class),
                $e->getMessage(),
            );
        }
    }

    public function test_maps_list_of_strings() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), WithTags::class);

        $result = $mapper->map([
            'id' => 1,
            'tags' => ['php', 'postgresql', 'flow'],
        ], MapperContextMother::any());

        self::assertSame(['php', 'postgresql', 'flow'], $result->tags);
    }

    public function test_maps_nested_object_from_array() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), UserWithAddress::class);

        $result = $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'address' => [
                'street' => 'Main 1',
                'city' => 'Warsaw',
            ],
        ], MapperContextMother::any());

        self::assertSame(1, $result->id);
        self::assertSame('Jane', $result->name);
        self::assertSame('Main 1', $result->address->street);
        self::assertSame('Warsaw', $result->address->city);
    }

    public function test_maps_nullable_property_when_value_is_null() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), NullableDto::class);

        $result = $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'nickname' => null,
        ], MapperContextMother::any());

        self::assertSame(1, $result->id);
        self::assertSame('Jane', $result->name);
        self::assertNull($result->nickname);
    }

    public function test_maps_row_to_simple_dto() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), SimpleDto::class);

        $result = $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'email' => 'jane@example.com',
        ], MapperContextMother::any());

        self::assertInstanceOf(SimpleDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertSame('Jane', $result->name);
        self::assertSame('jane@example.com', $result->email);
    }

    public function test_rejects_extra_columns_by_default() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), SimpleDto::class);

        $this->expectException(MappingException::class);

        $mapper->map([
            'id' => 1,
            'name' => 'Jane',
            'email' => 'jane@example.com',
            'extra' => 'boom',
        ], MapperContextMother::any());
    }

    public function test_wraps_mapping_error_as_mapping_exception_with_previous() : void
    {
        $mapper = new ValinorTreeMapper((new MapperBuilder())->mapper(), SimpleDto::class);

        try {
            $mapper->map(['id' => 'not-an-int'], MapperContextMother::any());
            self::fail('Expected MappingException was not thrown');
        } catch (MappingException $e) {
            self::assertInstanceOf(MappingError::class, $e->getPrevious());
        }
    }
}
