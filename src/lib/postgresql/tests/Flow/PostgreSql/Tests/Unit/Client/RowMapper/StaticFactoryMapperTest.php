<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper;

use function Flow\PostgreSql\DSL\static_factory_mapper;
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper\StaticFactoryMapper;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use PHPUnit\Framework\TestCase;

final class StaticFactoryMapperTest extends TestCase
{
    protected function setUp() : void
    {
        CapturedRowFactoryDto::$lastArgs = null;
    }

    public function test_accepts_but_ignores_context_argument() : void
    {
        $row = ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com'];

        $withAny = (new StaticFactoryMapper(SimpleFactoryDto::class, 'fromRow'))
            ->map($row, MapperContextMother::any());
        $withData = (new StaticFactoryMapper(SimpleFactoryDto::class, 'fromRow'))
            ->map($row, MapperContextMother::withData(['tenant_id' => 42]));

        self::assertEquals($withAny, $withData);
    }

    public function test_does_not_pass_context_to_factory() : void
    {
        $row = ['id' => 7];

        (new StaticFactoryMapper(CapturedRowFactoryDto::class, 'fromRow'))
            ->map($row, MapperContextMother::withData(['extra' => 'ignored']));

        self::assertNotNull(CapturedRowFactoryDto::$lastArgs);
        self::assertCount(1, CapturedRowFactoryDto::$lastArgs);
        self::assertSame($row, CapturedRowFactoryDto::$lastArgs[0]);
    }

    public function test_dsl_helper_returns_static_factory_mapper() : void
    {
        $row = ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'];

        $mapper = static_factory_mapper(SimpleFactoryDto::class, 'fromRow');

        self::assertInstanceOf(StaticFactoryMapper::class, $mapper);

        $result = $mapper->map($row, MapperContextMother::any());

        self::assertInstanceOf(SimpleFactoryDto::class, $result);
        self::assertSame(1, $result->id);
        self::assertSame('Alice', $result->name);
    }

    public function test_maps_row_to_object_via_static_factory() : void
    {
        $row = ['id' => 42, 'name' => 'John Doe', 'email' => 'john@example.com'];

        $result = (new StaticFactoryMapper(SimpleFactoryDto::class, 'fromRow'))
            ->map($row, MapperContextMother::any());

        self::assertInstanceOf(SimpleFactoryDto::class, $result);
        self::assertSame(42, $result->id);
        self::assertSame('John Doe', $result->name);
        self::assertSame('john@example.com', $result->email);
    }

    public function test_passes_row_unchanged_to_factory() : void
    {
        $row = ['id' => 1, 'name' => 'Bob', 'tags' => ['a', 'b'], 'meta' => null];

        (new StaticFactoryMapper(CapturedRowFactoryDto::class, 'fromRow'))
            ->map($row, MapperContextMother::any());

        self::assertNotNull(CapturedRowFactoryDto::$lastArgs);
        self::assertSame($row, CapturedRowFactoryDto::$lastArgs[0]);
    }

    public function test_returns_subclass_when_factory_returns_subclass() : void
    {
        $row = ['name' => 'Rex'];

        $result = (new StaticFactoryMapper(Animal::class, 'fromRow'))
            ->map($row, MapperContextMother::any());

        self::assertInstanceOf(Dog::class, $result);
        self::assertInstanceOf(Animal::class, $result);
        self::assertSame('Rex', $result->name);
    }

    public function test_throws_when_class_does_not_exist() : void
    {
        /** @var class-string<object> $nonExistent */
        $nonExistent = 'Flow\\PostgreSql\\Tests\\Unit\\Client\\RowMapper\\Fake\\NonExistentDto';

        $this->expectException(MappingException::class);
        $this->expectExceptionMessage('Class does not exist');

        new StaticFactoryMapper($nonExistent, 'fromRow');
    }

    public function test_throws_when_factory_method_does_not_exist() : void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage(\sprintf('Static factory method "%s::missing()" does not exist', SimpleFactoryDto::class));

        new StaticFactoryMapper(SimpleFactoryDto::class, 'missing');
    }

    public function test_throws_when_factory_method_is_not_public() : void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage(\sprintf('Factory method "%s::fromRow()" must be declared public', PrivateFactoryDto::class));

        new StaticFactoryMapper(PrivateFactoryDto::class, 'fromRow');
    }

    public function test_throws_when_factory_method_is_not_static() : void
    {
        $this->expectException(MappingException::class);
        $this->expectExceptionMessage(\sprintf('Factory method "%s::fromRow()" must be declared static', NonStaticFactoryDto::class));

        new StaticFactoryMapper(NonStaticFactoryDto::class, 'fromRow');
    }

    public function test_wraps_factory_throwable_in_mapping_exception() : void
    {
        try {
            (new StaticFactoryMapper(ThrowingFactoryDto::class, 'fromRow'))
                ->map(['id' => 1], MapperContextMother::any());
            self::fail('Expected MappingException was not thrown');
        } catch (MappingException $e) {
            self::assertStringContainsString('boom', $e->getMessage());
            self::assertInstanceOf(\RuntimeException::class, $e->getPrevious());
            self::assertSame('boom', $e->getPrevious()->getMessage());
        }
    }
}

final readonly class SimpleFactoryDto
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {
    }

    /**
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row) : self
    {
        return new self(
            id: (int) $row['id'],
            name: (string) $row['name'],
            email: (string) $row['email'],
        );
    }
}

final class CapturedRowFactoryDto
{
    /** @var null|array<int, mixed> */
    public static ?array $lastArgs = null;

    /**
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row) : self
    {
        self::$lastArgs = \func_get_args();

        return new self();
    }
}

final class NonStaticFactoryDto
{
    /**
     * @param array<string, mixed> $row
     */
    public function fromRow(array $row) : self
    {
        return new self();
    }
}

final class PrivateFactoryDto
{
    /**
     * @param array<string, mixed> $row
     */
    private static function fromRow(array $row) : self
    {
        return new self();
    }
}

final class ThrowingFactoryDto
{
    /**
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row) : self
    {
        throw new \RuntimeException('boom');
    }
}

class Animal
{
    public string $name = 'unknown';

    /**
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row) : self
    {
        $dog = new Dog();
        $dog->name = (string) $row['name'];

        return $dog;
    }
}

final class Dog extends Animal
{
}
