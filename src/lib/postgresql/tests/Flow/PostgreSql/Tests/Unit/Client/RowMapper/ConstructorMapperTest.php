<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper\ConstructorMapper;
use Flow\Types\Value\{Json, Uuid};
use PHPUnit\Framework\TestCase;

final class ConstructorMapperTest extends TestCase
{
    private ConstructorMapper $mapper;

    protected function setUp() : void
    {
        $this->mapper = new ConstructorMapper();
    }

    public function test_allows_extra_columns_in_row() : void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'email' => 'john@example.com',
            'extra_column' => 'ignored',
        ];

        $result = $this->mapper->map(SimpleDto::class, $row);

        self::assertInstanceOf(SimpleDto::class, $result);
        self::assertSame(1, $result->id);
    }

    public function test_handles_null_value_in_row() : void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'nickname' => null,
        ];

        $result = $this->mapper->map(NullableDto::class, $row);

        self::assertNull($result->nickname);
    }

    public function test_maps_all_supported_types() : void
    {
        $createdAt = new \DateTimeImmutable('2024-03-15 14:30:00');
        $metadata = Json::fromArray(['settings' => ['theme' => 'dark']]);
        $uuid = Uuid::fromString('550e8400-e29b-41d4-a716-446655440000');
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

        $result = $this->mapper->map(FullTypedDto::class, $row);

        self::assertInstanceOf(FullTypedDto::class, $result);
        self::assertSame(42, $result->id);
        self::assertSame('Test Entity', $result->name);
        self::assertSame(199.99, $result->price);
        self::assertTrue($result->active);
        self::assertSame($createdAt, $result->createdAt);
        self::assertSame($metadata, $result->metadata);
        self::assertSame($uuid, $result->uuid);
        self::assertSame($tags, $result->tags);
    }

    public function test_maps_array_type() : void
    {
        $tags = ['php', 'postgresql', 'flow'];
        $row = [
            'id' => 1,
            'tags' => $tags,
        ];

        $result = $this->mapper->map(ArrayDto::class, $row);

        self::assertInstanceOf(ArrayDto::class, $result);
        self::assertSame($tags, $result->tags);
    }

    public function test_maps_bool_type() : void
    {
        $row = [
            'id' => 1,
            'active' => true,
            'verified' => false,
        ];

        $result = $this->mapper->map(BoolDto::class, $row);

        self::assertInstanceOf(BoolDto::class, $result);
        self::assertTrue($result->active);
        self::assertFalse($result->verified);
    }

    public function test_maps_datetime_type() : void
    {
        $createdAt = new \DateTimeImmutable('2024-03-15 14:30:00');
        $row = [
            'id' => 1,
            'createdAt' => $createdAt,
        ];

        $result = $this->mapper->map(DateTimeDto::class, $row);

        self::assertInstanceOf(DateTimeDto::class, $result);
        self::assertSame($createdAt, $result->createdAt);
    }

    public function test_maps_float_special_values() : void
    {
        $row = [
            'id' => 1,
            'price' => \INF,
            'discount' => \NAN,
        ];

        $result = $this->mapper->map(FloatDto::class, $row);

        self::assertInstanceOf(FloatDto::class, $result);
        self::assertSame(\INF, $result->price);
        self::assertNan($result->discount);
    }

    public function test_maps_float_type() : void
    {
        $row = [
            'id' => 1,
            'price' => 99.99,
            'discount' => 0.15,
        ];

        $result = $this->mapper->map(FloatDto::class, $row);

        self::assertInstanceOf(FloatDto::class, $result);
        self::assertSame(99.99, $result->price);
        self::assertSame(0.15, $result->discount);
    }

    public function test_maps_json_type() : void
    {
        $json = Json::fromArray(['name' => 'John', 'age' => 30]);
        $row = [
            'id' => 1,
            'metadata' => $json,
        ];

        $result = $this->mapper->map(JsonDto::class, $row);

        self::assertInstanceOf(JsonDto::class, $result);
        self::assertSame($json, $result->metadata);
        self::assertSame(['name' => 'John', 'age' => 30], $result->metadata->toArray());
    }

    public function test_maps_nullable_complex_types() : void
    {
        $row = [
            'id' => 1,
        ];

        $result = $this->mapper->map(NullableTypedDto::class, $row);

        self::assertInstanceOf(NullableTypedDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertNull($result->createdAt);
        self::assertNull($result->metadata);
        self::assertNull($result->uuid);
        self::assertNull($result->tags);
    }

    public function test_maps_nullable_complex_types_with_values() : void
    {
        $createdAt = new \DateTimeImmutable('2024-01-01');
        $metadata = Json::fromArray(['key' => 'value']);
        $uuid = Uuid::fromString('550e8400-e29b-41d4-a716-446655440000');
        $tags = ['tag1', 'tag2'];

        $row = [
            'id' => 1,
            'createdAt' => $createdAt,
            'metadata' => $metadata,
            'uuid' => $uuid,
            'tags' => $tags,
        ];

        $result = $this->mapper->map(NullableTypedDto::class, $row);

        self::assertInstanceOf(NullableTypedDto::class, $result);
        self::assertSame($createdAt, $result->createdAt);
        self::assertSame($metadata, $result->metadata);
        self::assertSame($uuid, $result->uuid);
        self::assertSame($tags, $result->tags);
    }

    public function test_maps_nullable_parameters() : void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
        ];

        $result = $this->mapper->map(NullableDto::class, $row);

        self::assertInstanceOf(NullableDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertSame('John Doe', $result->name);
        self::assertNull($result->nickname);
    }

    public function test_maps_row_to_simple_dto() : void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
            'email' => 'john@example.com',
        ];

        $result = $this->mapper->map(SimpleDto::class, $row);

        self::assertInstanceOf(SimpleDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertSame('John Doe', $result->name);
        self::assertSame('john@example.com', $result->email);
    }

    public function test_maps_uuid_type() : void
    {
        $uuid = Uuid::fromString('550e8400-e29b-41d4-a716-446655440000');
        $row = [
            'id' => 1,
            'uuid' => $uuid,
        ];

        $result = $this->mapper->map(UuidDto::class, $row);

        self::assertInstanceOf(UuidDto::class, $result);
        self::assertSame($uuid, $result->uuid);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $result->uuid->toString());
    }

    public function test_throws_for_class_without_constructor() : void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Class has no constructor');

        $this->mapper->map(NoConstructorDto::class, ['id' => 1]);
    }

    public function test_throws_for_invalid_class() : void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Class does not exist');

        /** @phpstan-ignore argument.type */
        $this->mapper->map('NonExistentClass', ['id' => 1]);
    }

    public function test_throws_for_missing_required_parameter() : void
    {
        $row = [
            'id' => 1,
        ];

        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Property "name" not found');

        $this->mapper->map(SimpleDto::class, $row);
    }

    public function test_uses_default_values() : void
    {
        $row = [
            'id' => 1,
            'name' => 'John Doe',
        ];

        $result = $this->mapper->map(DefaultValueDto::class, $row);

        self::assertInstanceOf(DefaultValueDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertSame('John Doe', $result->name);
        self::assertTrue($result->active);
    }
}

final readonly class SimpleDto
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {
    }
}

final readonly class NullableDto
{
    public function __construct(
        public int $id,
        public string $name,
        public ?string $nickname = null,
    ) {
    }
}

final readonly class DefaultValueDto
{
    public function __construct(
        public int $id,
        public string $name,
        public bool $active = true,
    ) {
    }
}

final class NoConstructorDto
{
    public int $id;
}

final readonly class JsonDto
{
    public function __construct(
        public int $id,
        public Json $metadata,
    ) {
    }
}

final readonly class UuidDto
{
    public function __construct(
        public int $id,
        public Uuid $uuid,
    ) {
    }
}

final readonly class DateTimeDto
{
    public function __construct(
        public int $id,
        public \DateTimeImmutable $createdAt,
    ) {
    }
}

final readonly class FloatDto
{
    public function __construct(
        public int $id,
        public float $price,
        public float $discount,
    ) {
    }
}

final readonly class BoolDto
{
    public function __construct(
        public int $id,
        public bool $active,
        public bool $verified,
    ) {
    }
}

final readonly class ArrayDto
{
    /**
     * @param list<string> $tags
     */
    public function __construct(
        public int $id,
        public array $tags,
    ) {
    }
}

final readonly class NullableTypedDto
{
    /**
     * @param null|list<string> $tags
     */
    public function __construct(
        public int $id,
        public ?\DateTimeImmutable $createdAt = null,
        public ?Json $metadata = null,
        public ?Uuid $uuid = null,
        public ?array $tags = null,
    ) {
    }
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
        public Json $metadata,
        public Uuid $uuid,
        public array $tags,
    ) {
    }
}
