<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Attributes;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class AttributesTest extends TestCase
{
    public static function id_provider(): \Generator
    {
        yield 'empty attributes' => [
            [],
            '',
        ];

        yield 'single string' => [
            ['name' => 'Alice'],
            'name=Alice',
        ];

        yield 'single int' => [
            ['count' => 42],
            'count=42',
        ];

        yield 'single float' => [
            ['ratio' => 3.14],
            'ratio=3.14',
        ];

        yield 'bool true' => [
            ['enabled' => true],
            'enabled=true',
        ];

        yield 'bool false' => [
            ['enabled' => false],
            'enabled=false',
        ];

        yield 'multiple scalars sorted by key' => [
            ['z' => 'last', 'a' => 'first', 'm' => 'middle'],
            'a=first|m=middle|z=last',
        ];

        yield 'mixed types sorted by key' => [
            ['method' => 'GET', 'status' => 200, 'cached' => true],
            'cached=true|method=GET|status=200',
        ];

        yield 'null values are excluded' => [
            ['key' => 'value', 'empty' => null],
            'key=value',
        ];

        yield 'array values are excluded' => [
            ['tags' => ['a', 'b'], 'name' => 'test'],
            'name=test',
        ];

        yield 'all non-scalar values excluded results in empty id' => [
            ['tags' => ['a', 'b']],
            '',
        ];

        yield 'datetime is formatted as ISO 8601' => [
            ['ts' => new \DateTimeImmutable('2024-01-15T10:30:00+00:00')],
            'ts=2024-01-15T10:30:00+00:00',
        ];

        yield 'throwable uses message' => [
            ['error' => new \RuntimeException('something broke')],
            'error=something broke',
        ];

        yield 'same keys different order produce same id' => [
            ['b' => '2', 'a' => '1'],
            'a=1|b=2',
        ];
    }

    public function test_constructor_filters_null_values(): void
    {
        $attributes = Attributes::create([
            'key1' => 'value1',
            'key2' => null,
            'key3' => 0,
            'key4' => '',
            'key5' => null,
        ]);

        static::assertSame(['key1' => 'value1', 'key3' => 0, 'key4' => ''], $attributes->normalize());
    }

    public function test_count_returns_correct_number(): void
    {
        $attributes = Attributes::create(['a' => '1', 'b' => '2', 'c' => '3']);

        static::assertSame(3, $attributes->count());
    }

    public function test_create_empty(): void
    {
        $attributes = Attributes::create();

        static::assertTrue($attributes->isEmpty());
        static::assertSame(0, $attributes->count());
    }

    public function test_create_with_values(): void
    {
        $values = [
            'user.id' => '12345',
            'user.name' => 'John',
        ];

        $attributes = Attributes::create($values);

        static::assertSame('12345', $attributes->get('user.id'));
        static::assertSame('John', $attributes->get('user.name'));
    }

    public function test_empty_factory_method(): void
    {
        $attributes = Attributes::empty();

        static::assertTrue($attributes->isEmpty());
        static::assertSame([], $attributes->normalize());
    }

    public function test_from_array(): void
    {
        $data = [
            'key1' => 'value1',
            'key2' => 42,
        ];

        $attributes = Attributes::fromArray($data);

        static::assertSame('value1', $attributes->get('key1'));
        static::assertSame(42, $attributes->get('key2'));
    }

    public function test_get_existing_attribute(): void
    {
        $attributes = Attributes::create(['key' => 'value']);

        static::assertSame('value', $attributes->get('key'));
    }

    public function test_get_non_existing_attribute_returns_null(): void
    {
        $attributes = Attributes::create();

        static::assertNull($attributes->get('missing'));
    }

    public function test_has_returns_false_for_non_existing(): void
    {
        $attributes = Attributes::create();

        static::assertFalse($attributes->has('missing'));
    }

    public function test_has_returns_true_for_existing(): void
    {
        $attributes = Attributes::create(['key' => 'value']);

        static::assertTrue($attributes->has('key'));
    }

    /**
     * @param array<string, null|array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable> $values
     */
    #[DataProvider('id_provider')]
    public function test_id(array $values, string $expectedId): void
    {
        static::assertSame($expectedId, Attributes::create($values)->id());
    }

    public function test_id_is_stable_regardless_of_insertion_order(): void
    {
        $a = Attributes::create(['x' => '1', 'y' => '2', 'z' => '3']);
        $b = Attributes::create(['z' => '3', 'x' => '1', 'y' => '2']);

        static::assertSame($a->id(), $b->id());
    }

    public function test_is_empty_for_empty_attributes(): void
    {
        $attributes = Attributes::empty();

        static::assertTrue($attributes->isEmpty());
    }

    public function test_is_not_empty_for_attributes_with_values(): void
    {
        $attributes = Attributes::create(['key' => 'value']);

        static::assertFalse($attributes->isEmpty());
    }

    public function test_merge_combines_attributes(): void
    {
        $attributes1 = Attributes::create(['a' => '1', 'b' => '2']);
        $attributes2 = Attributes::create(['c' => '3']);

        $merged = $attributes1->merge($attributes2);

        static::assertSame('1', $merged->get('a'));
        static::assertSame('2', $merged->get('b'));
        static::assertSame('3', $merged->get('c'));
    }

    public function test_merge_is_immutable(): void
    {
        $attributes1 = Attributes::create(['a' => '1']);
        $attributes2 = Attributes::create(['b' => '2']);

        $merged = $attributes1->merge($attributes2);

        static::assertNotSame($attributes1, $merged);
        static::assertNotSame($attributes2, $merged);
        static::assertFalse($attributes1->has('b'));
        static::assertFalse($attributes2->has('a'));
    }

    public function test_merge_other_takes_precedence(): void
    {
        $attributes1 = Attributes::create(['key' => 'original']);
        $attributes2 = Attributes::create(['key' => 'override']);

        $merged = $attributes1->merge($attributes2);

        static::assertSame('override', $merged->get('key'));
    }

    public function test_normalize_from_array_roundtrip(): void
    {
        $original = Attributes::create([
            'string' => 'value',
            'int' => 42,
            'float' => 3.14,
            'bool' => true,
            'array' => ['a', 'b', 'c'],
        ]);

        $normalized = $original->normalize();
        $restored = Attributes::fromArray($normalized);

        static::assertSame($original->normalize(), $restored->normalize());
    }

    public function test_normalize_returns_raw_array(): void
    {
        $values = [
            'key1' => 'value1',
            'key2' => 42,
        ];
        $attributes = Attributes::create($values);

        $normalized = $attributes->normalize();

        static::assertSame($values, $normalized);
    }

    public function test_supports_array_values(): void
    {
        $attributes = Attributes::create([
            'tags' => ['web', 'api', 'v2'],
        ]);

        static::assertSame(['web', 'api', 'v2'], $attributes->get('tags'));
    }

    public function test_supports_bool_values(): void
    {
        $attributes = Attributes::create([
            'enabled' => true,
            'disabled' => false,
        ]);

        static::assertTrue($attributes->get('enabled'));
        static::assertFalse($attributes->get('disabled'));
    }

    public function test_supports_float_values(): void
    {
        $attributes = Attributes::create([
            'ratio' => 3.14159,
        ]);

        static::assertSame(3.14159, $attributes->get('ratio'));
    }

    public function test_supports_int_values(): void
    {
        $attributes = Attributes::create([
            'count' => 42,
        ]);

        static::assertSame(42, $attributes->get('count'));
    }

    public function test_supports_nested_arrays(): void
    {
        $attributes = Attributes::create([
            'nested' => ['a', 'b', 'c'],
            'mixed' => [1, 'two', 3.0, true],
        ]);

        static::assertSame(['a', 'b', 'c'], $attributes->get('nested'));
        static::assertSame([1, 'two', 3.0, true], $attributes->get('mixed'));
    }

    public function test_supports_string_values(): void
    {
        $attributes = Attributes::create([
            'name' => 'John Doe',
        ]);

        static::assertSame('John Doe', $attributes->get('name'));
    }

    public function test_to_array_is_alias_for_normalize(): void
    {
        $attributes = Attributes::create([
            'key' => 'value',
        ]);

        static::assertSame($attributes->normalize(), $attributes->normalize());
    }

    public function test_with_adds_attribute(): void
    {
        $attributes = Attributes::create();
        $newAttributes = $attributes->with('key', 'value');

        static::assertFalse($attributes->has('key'));
        static::assertTrue($newAttributes->has('key'));
        static::assertSame('value', $newAttributes->get('key'));
    }

    public function test_with_is_immutable(): void
    {
        $original = Attributes::create(['key' => 'value']);

        $withAdded = $original->with('new', 'entry');

        static::assertNotSame($original, $withAdded);
        static::assertSame(1, $original->count());
        static::assertSame(2, $withAdded->count());
    }

    public function test_with_replaces_existing_attribute(): void
    {
        $attributes = Attributes::create(['key' => 'old']);
        $newAttributes = $attributes->with('key', 'new');

        static::assertSame('old', $attributes->get('key'));
        static::assertSame('new', $newAttributes->get('key'));
    }

    public function test_with_supports_various_types(): void
    {
        $attributes = Attributes::create()
            ->with('string', 'value')
            ->with('int', 42)
            ->with('float', 3.14)
            ->with('bool', true)
            ->with('array', ['a', 'b', 'c']);

        static::assertSame('value', $attributes->get('string'));
        static::assertSame(42, $attributes->get('int'));
        static::assertSame(3.14, $attributes->get('float'));
        static::assertTrue($attributes->get('bool'));
        static::assertSame(['a', 'b', 'c'], $attributes->get('array'));
    }
}
