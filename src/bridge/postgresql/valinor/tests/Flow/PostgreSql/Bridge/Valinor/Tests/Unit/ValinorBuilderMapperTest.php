<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit;

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_integer, type_string, type_structure};
use CuyZ\Valinor\Mapper\MappingError;
use CuyZ\Valinor\MapperBuilder;
use Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture\{Address, SimpleDto, UserWithAddress};
use Flow\PostgreSql\Bridge\Valinor\ValinorBuilderMapper;
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use PHPUnit\Framework\TestCase;

final class ValinorBuilderMapperTest extends TestCase
{
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
            new ValinorBuilderMapper(new MapperBuilder(), UserWithAddress::class),
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

    public function test_map_succeeds_twice_on_same_instance() : void
    {
        $mapper = new ValinorBuilderMapper(new MapperBuilder(), SimpleDto::class);

        $first = $mapper->map(['id' => 1, 'name' => 'Jane', 'email' => 'jane@example.com'], MapperContextMother::any());
        $second = $mapper->map(['id' => 2, 'name' => 'John', 'email' => 'john@example.com'], MapperContextMother::any());

        self::assertSame(1, $first->id);
        self::assertSame(2, $second->id);
    }

    public function test_maps_row_to_simple_dto() : void
    {
        $mapper = new ValinorBuilderMapper(new MapperBuilder(), SimpleDto::class);

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

    public function test_wraps_mapping_error_as_mapping_exception_with_previous() : void
    {
        $mapper = new ValinorBuilderMapper(new MapperBuilder(), SimpleDto::class);

        try {
            $mapper->map(['id' => 'not-an-int'], MapperContextMother::any());
            self::fail('Expected MappingException was not thrown');
        } catch (MappingException $e) {
            self::assertInstanceOf(MappingError::class, $e->getPrevious());
            self::assertStringStartsWith(
                \sprintf('Failed to map row to "%s":', SimpleDto::class),
                $e->getMessage(),
            );
        }
    }
}
