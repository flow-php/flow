<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;

final class UuidConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{Uuid, string}>
     */
    public static function provide_uuid_objects() : \Generator
    {
        yield 'uuid object' => [
            new Uuid('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
        ];
        yield 'nil uuid object' => [
            new Uuid('00000000-0000-0000-0000-000000000000'),
            '00000000-0000-0000-0000-000000000000',
        ];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_uuid_strings() : \Generator
    {
        yield 'lowercase' => ['550e8400-e29b-41d4-a716-446655440000'];
        yield 'uppercase' => ['550E8400-E29B-41D4-A716-446655440000'];
        yield 'nil uuid' => ['00000000-0000-0000-0000-000000000000'];
        yield 'max uuid' => ['ffffffff-ffff-ffff-ffff-ffffffffffff'];
    }

    public function test_gen_random_uuid() : void
    {
        $result = $this->fetchValue('SELECT gen_random_uuid() AS val');

        self::assertInstanceOf(Uuid::class, $result);
        self::assertTrue(Uuid::isValid($result->toString()));
    }

    public function test_null_uuid() : void
    {
        $result = $this->fetchValue('SELECT NULL::uuid AS val');

        self::assertNull($result);
    }

    #[DataProvider('provide_uuid_objects')]
    public function test_uuid_object_round_trip(Uuid $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::uuid AS val', [$input]);

        self::assertInstanceOf(Uuid::class, $result);
        self::assertSame($expected, $result->toString());
    }

    #[DataProvider('provide_uuid_strings')]
    public function test_uuid_string_round_trip(string $input) : void
    {
        $result = $this->fetchValue('SELECT $1::uuid AS val', [$input]);

        self::assertInstanceOf(Uuid::class, $result);
        self::assertSame(\strtolower($input), $result->toString());
    }
}
