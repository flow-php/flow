<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper\ConstructorMapper;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use PHPUnit\Framework\TestCase;

final class ConstructorMapperTest extends TestCase
{
    public function test_allows_extra_columns_in_row(): void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'email' => 'john@example.com',
            'extra_column' => 'ignored',
        ];

        $result = (new ConstructorMapper(SimpleDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(SimpleDto::class, $result);
        static::assertSame(1, $result->id);
    }

    public function test_handles_null_value_in_row(): void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'nickname' => null,
        ];

        $result = (new ConstructorMapper(NullableDto::class))->map($row, MapperContextMother::any());

        static::assertNull($result->nickname);
    }

    public function test_maps_all_supported_types(): void
    {
        $createdAt = new \DateTimeImmutable('2024-03-15 14:30:00');
        $metadata = '{"settings":{"theme":"dark"}}';
        $uuid = '550e8400-e29b-41d4-a716-446655440000';
        $tags = ['important', 'urgent'];

        $row = [
            'id' => 42,
            'name' => 'Test Entity',
            'price' => 199.99,
            'active' => true,
            'createdAt' => $createdAt,
            'metadata' => $metadata,
            'uuid' => $uuid,
            'tags' => $tags,
        ];

        $result = (new ConstructorMapper(FullTypedDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(FullTypedDto::class, $result);
        static::assertSame(42, $result->id);
        static::assertSame('Test Entity', $result->name);
        static::assertSame(199.99, $result->price);
        static::assertTrue($result->active);
        static::assertSame($createdAt, $result->createdAt);
        static::assertSame($metadata, $result->metadata);
        static::assertSame($uuid, $result->uuid);
        static::assertSame($tags, $result->tags);
    }

    public function test_maps_array_type(): void
    {
        $tags = ['php', 'postgresql', 'flow'];
        $row = [
            'id' => 1,
            'tags' => $tags,
        ];

        $result = (new ConstructorMapper(ArrayDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(ArrayDto::class, $result);
        static::assertSame($tags, $result->tags);
    }

    public function test_maps_bool_type(): void
    {
        $row = [
            'id' => 1,
            'active' => true,
            'verified' => false,
        ];

        $result = (new ConstructorMapper(BoolDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(BoolDto::class, $result);
        static::assertTrue($result->active);
        static::assertFalse($result->verified);
    }

    public function test_maps_datetime_type(): void
    {
        $createdAt = new \DateTimeImmutable('2024-03-15 14:30:00');
        $row = [
            'id' => 1,
            'createdAt' => $createdAt,
        ];

        $result = (new ConstructorMapper(DateTimeDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(DateTimeDto::class, $result);
        static::assertSame($createdAt, $result->createdAt);
    }

    public function test_maps_float_special_values(): void
    {
        $row = [
            'id' => 1,
            'price' => \INF,
            'discount' => \NAN,
        ];

        $result = (new ConstructorMapper(FloatDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(FloatDto::class, $result);
        static::assertSame(\INF, $result->price);
        static::assertNan($result->discount);
    }

    public function test_maps_float_type(): void
    {
        $row = [
            'id' => 1,
            'price' => 99.99,
            'discount' => 0.15,
        ];

        $result = (new ConstructorMapper(FloatDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(FloatDto::class, $result);
        static::assertSame(99.99, $result->price);
        static::assertSame(0.15, $result->discount);
    }

    public function test_maps_json_type(): void
    {
        $json = '{"name":"John","age":30}';
        $row = [
            'id' => 1,
            'metadata' => $json,
        ];

        $result = (new ConstructorMapper(JsonDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(JsonDto::class, $result);
        static::assertSame($json, $result->metadata);
        static::assertSame(
            ['name' => 'John', 'age' => 30],
            \json_decode($result->metadata, true, 512, \JSON_THROW_ON_ERROR),
        );
    }

    public function test_maps_nullable_complex_types(): void
    {
        $row = [
            'id' => 1,
        ];

        $result = (new ConstructorMapper(NullableTypedDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(NullableTypedDto::class, $result);
        static::assertSame(1, $result->id);
        static::assertNull($result->createdAt);
        static::assertNull($result->metadata);
        static::assertNull($result->uuid);
        static::assertNull($result->tags);
    }

    public function test_maps_nullable_complex_types_with_values(): void
    {
        $createdAt = new \DateTimeImmutable('2024-01-01');
        $metadata = '{"key":"value"}';
        $uuid = '550e8400-e29b-41d4-a716-446655440000';
        $tags = ['tag1', 'tag2'];

        $row = [
            'id' => 1,
            'createdAt' => $createdAt,
            'metadata' => $metadata,
            'uuid' => $uuid,
            'tags' => $tags,
        ];

        $result = (new ConstructorMapper(NullableTypedDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(NullableTypedDto::class, $result);
        static::assertSame($createdAt, $result->createdAt);
        static::assertSame($metadata, $result->metadata);
        static::assertSame($uuid, $result->uuid);
        static::assertSame($tags, $result->tags);
    }

    public function test_maps_nullable_parameters(): void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
        ];

        $result = (new ConstructorMapper(NullableDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(NullableDto::class, $result);
        static::assertSame(1, $result->id);
        static::assertSame('John Doe', $result->name);
        static::assertNull($result->nickname);
    }

    public function test_maps_row_to_simple_dto(): void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'email' => 'john@example.com',
        ];

        $result = (new ConstructorMapper(SimpleDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(SimpleDto::class, $result);
        static::assertSame(1, $result->id);
        static::assertSame('John Doe', $result->name);
        static::assertSame('john@example.com', $result->email);
    }

    public function test_maps_uuid_type(): void
    {
        $uuid = '550e8400-e29b-41d4-a716-446655440000';
        $row = [
            'id' => 1,
            'uuid' => $uuid,
        ];

        $result = (new ConstructorMapper(UuidDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(UuidDto::class, $result);
        static::assertSame($uuid, $result->uuid);
    }

    public function test_throws_for_class_without_constructor(): void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Class has no constructor');

        new ConstructorMapper(NoConstructorDto::class);
    }

    public function test_throws_for_missing_required_parameter(): void
    {
        $row = [
            'id' => 1,
        ];

        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Property "name" not found');

        (new ConstructorMapper(SimpleDto::class))->map($row, MapperContextMother::any());
    }

    public function test_uses_default_values(): void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
        ];

        $result = (new ConstructorMapper(DefaultValueDto::class))->map($row, MapperContextMother::any());

        static::assertInstanceOf(DefaultValueDto::class, $result);
        static::assertSame(1, $result->id);
        static::assertSame('John Doe', $result->name);
        static::assertTrue($result->active);
    }
}

final readonly class SimpleDto
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {}
}

final readonly class NullableDto
{
    public function __construct(
        public int $id,
        public string $name,
        public ?string $nickname = null,
    ) {}
}

final readonly class DefaultValueDto
{
    public function __construct(
        public int $id,
        public string $name,
        public bool $active = true,
    ) {}
}

final class NoConstructorDto
{
    public int $id;
}

final readonly class JsonDto
{
    public function __construct(
        public int $id,
        public string $metadata,
    ) {}
}

final readonly class UuidDto
{
    public function __construct(
        public int $id,
        public string $uuid,
    ) {}
}

final readonly class DateTimeDto
{
    public function __construct(
        public int $id,
        public \DateTimeImmutable $createdAt,
    ) {}
}

final readonly class FloatDto
{
    public function __construct(
        public int $id,
        public float $price,
        public float $discount,
    ) {}
}

final readonly class BoolDto
{
    public function __construct(
        public int $id,
        public bool $active,
        public bool $verified,
    ) {}
}

final readonly class ArrayDto
{
    /**
     * @param list<string> $tags
     */
    public function __construct(
        public int $id,
        public array $tags,
    ) {}
}

final readonly class NullableTypedDto
{
    /**
     * @param null|list<string> $tags
     */
    public function __construct(
        public int $id,
        public ?\DateTimeImmutable $createdAt = null,
        public ?string $metadata = null,
        public ?string $uuid = null,
        public ?array $tags = null,
    ) {}
}

final readonly class FullTypedDto
{
    /**
     * @param list<string> $tags
     */
    public function __construct(
        public int $id,
        public string $name,
        public float $price,
        public bool $active,
        public \DateTimeImmutable $createdAt,
        public string $metadata,
        public string $uuid,
        public array $tags,
    ) {}
}
