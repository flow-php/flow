<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Attributes;
use PHPUnit\Framework\TestCase;

final class AttributesTest extends TestCase
{
    public function test_count_returns_correct_number() : void
    {
        $attributes = Attributes::create(['a' => '1', 'b' => '2', 'c' => '3']);

        self::assertSame(3, $attributes->count());
    }

    public function test_create_empty() : void
    {
        $attributes = Attributes::create();

        self::assertTrue($attributes->isEmpty());
        self::assertSame(0, $attributes->count());
    }

    public function test_create_with_values() : void
    {
        $values = [
            'user.id' => '12345',
            'user.name' => 'John',
        ];

        $attributes = Attributes::create($values);

        self::assertSame('12345', $attributes->get('user.id'));
        self::assertSame('John', $attributes->get('user.name'));
    }

    public function test_empty_factory_method() : void
    {
        $attributes = Attributes::empty();

        self::assertTrue($attributes->isEmpty());
        self::assertSame([], $attributes->normalize());
    }

    public function test_from_array() : void
    {
        $data = [
            'key1' => 'value1',
            'key2' => 42,
        ];

        $attributes = Attributes::fromArray($data);

        self::assertSame('value1', $attributes->get('key1'));
        self::assertSame(42, $attributes->get('key2'));
    }

    public function test_get_existing_attribute() : void
    {
        $attributes = Attributes::create(['key' => 'value']);

        self::assertSame('value', $attributes->get('key'));
    }

    public function test_get_non_existing_attribute_returns_null() : void
    {
        $attributes = Attributes::create();

        self::assertNull($attributes->get('missing'));
    }

    public function test_has_returns_false_for_non_existing() : void
    {
        $attributes = Attributes::create();

        self::assertFalse($attributes->has('missing'));
    }

    public function test_has_returns_true_for_existing() : void
    {
        $attributes = Attributes::create(['key' => 'value']);

        self::assertTrue($attributes->has('key'));
    }

    public function test_is_empty_for_empty_attributes() : void
    {
        $attributes = Attributes::empty();

        self::assertTrue($attributes->isEmpty());
    }

    public function test_is_not_empty_for_attributes_with_values() : void
    {
        $attributes = Attributes::create(['key' => 'value']);

        self::assertFalse($attributes->isEmpty());
    }

    public function test_merge_combines_attributes() : void
    {
        $attributes1 = Attributes::create(['a' => '1', 'b' => '2']);
        $attributes2 = Attributes::create(['c' => '3']);

        $merged = $attributes1->merge($attributes2);

        self::assertSame('1', $merged->get('a'));
        self::assertSame('2', $merged->get('b'));
        self::assertSame('3', $merged->get('c'));
    }

    public function test_merge_is_immutable() : void
    {
        $attributes1 = Attributes::create(['a' => '1']);
        $attributes2 = Attributes::create(['b' => '2']);

        $merged = $attributes1->merge($attributes2);

        self::assertNotSame($attributes1, $merged);
        self::assertNotSame($attributes2, $merged);
        self::assertFalse($attributes1->has('b'));
        self::assertFalse($attributes2->has('a'));
    }

    public function test_merge_other_takes_precedence() : void
    {
        $attributes1 = Attributes::create(['key' => 'original']);
        $attributes2 = Attributes::create(['key' => 'override']);

        $merged = $attributes1->merge($attributes2);

        self::assertSame('override', $merged->get('key'));
    }

    public function test_normalize_from_array_roundtrip() : void
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

        self::assertSame($original->normalize(), $restored->normalize());
    }

    public function test_normalize_returns_raw_array() : void
    {
        $values = [
            'key1' => 'value1',
            'key2' => 42,
        ];
        $attributes = Attributes::create($values);

        $normalized = $attributes->normalize();

        self::assertSame($values, $normalized);
    }

    public function test_supports_array_values() : void
    {
        $attributes = Attributes::create([
            'tags' => ['web', 'api', 'v2'],
        ]);

        self::assertSame(['web', 'api', 'v2'], $attributes->get('tags'));
    }

    public function test_supports_bool_values() : void
    {
        $attributes = Attributes::create([
            'enabled' => true,
            'disabled' => false,
        ]);

        self::assertTrue($attributes->get('enabled'));
        self::assertFalse($attributes->get('disabled'));
    }

    public function test_supports_float_values() : void
    {
        $attributes = Attributes::create([
            'ratio' => 3.14159,
        ]);

        self::assertSame(3.14159, $attributes->get('ratio'));
    }

    public function test_supports_int_values() : void
    {
        $attributes = Attributes::create([
            'count' => 42,
        ]);

        self::assertSame(42, $attributes->get('count'));
    }

    public function test_supports_nested_arrays() : void
    {
        $attributes = Attributes::create([
            'nested' => ['a', 'b', 'c'],
            'mixed' => [1, 'two', 3.0, true],
        ]);

        self::assertSame(['a', 'b', 'c'], $attributes->get('nested'));
        self::assertSame([1, 'two', 3.0, true], $attributes->get('mixed'));
    }

    public function test_supports_string_values() : void
    {
        $attributes = Attributes::create([
            'name' => 'John Doe',
        ]);

        self::assertSame('John Doe', $attributes->get('name'));
    }

    public function test_to_array_is_alias_for_normalize() : void
    {
        $attributes = Attributes::create([
            'key' => 'value',
        ]);

        self::assertSame($attributes->normalize(), $attributes->normalize());
    }

    public function test_with_adds_attribute() : void
    {
        $attributes = Attributes::create();
        $newAttributes = $attributes->with('key', 'value');

        self::assertFalse($attributes->has('key'));
        self::assertTrue($newAttributes->has('key'));
        self::assertSame('value', $newAttributes->get('key'));
    }

    public function test_with_is_immutable() : void
    {
        $original = Attributes::create(['key' => 'value']);

        $withAdded = $original->with('new', 'entry');

        self::assertNotSame($original, $withAdded);
        self::assertSame(1, $original->count());
        self::assertSame(2, $withAdded->count());
    }

    public function test_with_replaces_existing_attribute() : void
    {
        $attributes = Attributes::create(['key' => 'old']);
        $newAttributes = $attributes->with('key', 'new');

        self::assertSame('old', $attributes->get('key'));
        self::assertSame('new', $newAttributes->get('key'));
    }

    public function test_with_supports_various_types() : void
    {
        $attributes = Attributes::create()
            ->with('string', 'value')
            ->with('int', 42)
            ->with('float', 3.14)
            ->with('bool', true)
            ->with('array', ['a', 'b', 'c']);

        self::assertSame('value', $attributes->get('string'));
        self::assertSame(42, $attributes->get('int'));
        self::assertSame(3.14, $attributes->get('float'));
        self::assertTrue($attributes->get('bool'));
        self::assertSame(['a', 'b', 'c'], $attributes->get('array'));
    }
}
