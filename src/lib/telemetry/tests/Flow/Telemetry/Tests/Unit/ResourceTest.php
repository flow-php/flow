<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Resource;
use PHPUnit\Framework\TestCase;

final class ResourceTest extends TestCase
{
    public function test_all_returns_all_attributes() : void
    {
        $attributes = [
            'a' => '1',
            'b' => 2,
            'c' => true,
        ];
        $resource = Resource::create($attributes);

        self::assertSame($attributes, $resource->all());
    }

    public function test_chained_operations() : void
    {
        $resource = Resource::create()
            ->with('service.name', 'my-service')
            ->with('service.version', '1.0.0')
            ->with('host.name', 'localhost');

        self::assertSame(3, $resource->count());
        self::assertSame('my-service', $resource->get('service.name'));
        self::assertSame('1.0.0', $resource->get('service.version'));
        self::assertSame('localhost', $resource->get('host.name'));
    }

    public function test_count_returns_correct_number() : void
    {
        $resource = Resource::create(['a' => '1', 'b' => '2', 'c' => '3']);

        self::assertSame(3, $resource->count());
    }

    public function test_create_empty_resource() : void
    {
        $resource = Resource::create();

        self::assertTrue($resource->isEmpty());
        self::assertSame(0, $resource->count());
    }

    public function test_create_with_attributes() : void
    {
        $attributes = [
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ];

        $resource = Resource::create($attributes);

        self::assertSame('my-service', $resource->get('service.name'));
        self::assertSame('1.0.0', $resource->get('service.version'));
    }

    public function test_empty_factory_method() : void
    {
        $resource = Resource::empty();

        self::assertTrue($resource->isEmpty());
        self::assertSame([], $resource->all());
    }

    public function test_from_array_creates_resource() : void
    {
        $data = [
            'attributes' => [
                'service.name' => 'my-service',
                'service.version' => '1.0.0',
            ],
        ];

        $resource = Resource::fromArray($data);

        self::assertSame('my-service', $resource->get('service.name'));
        self::assertSame('1.0.0', $resource->get('service.version'));
        self::assertSame(2, $resource->count());
    }

    public function test_from_array_with_empty_attributes() : void
    {
        $data = ['attributes' => []];

        $resource = Resource::fromArray($data);

        self::assertTrue($resource->isEmpty());
    }

    public function test_get_returns_null_for_missing_key() : void
    {
        $resource = Resource::create();

        self::assertNull($resource->get('missing'));
    }

    public function test_has_returns_false_for_missing_key() : void
    {
        $resource = Resource::create();

        self::assertFalse($resource->has('missing'));
    }

    public function test_has_returns_true_for_existing_key() : void
    {
        $resource = Resource::create(['key' => 'value']);

        self::assertTrue($resource->has('key'));
    }

    public function test_is_empty_returns_false_for_non_empty_resource() : void
    {
        $resource = Resource::create(['key' => 'value']);

        self::assertFalse($resource->isEmpty());
    }

    public function test_is_empty_returns_true_for_empty_resource() : void
    {
        $resource = Resource::empty();

        self::assertTrue($resource->isEmpty());
    }

    public function test_merge_combines_resources() : void
    {
        $resource1 = Resource::create(['a' => '1', 'b' => '2']);
        $resource2 = Resource::create(['c' => '3']);

        $merged = $resource1->merge($resource2);

        self::assertSame('1', $merged->get('a'));
        self::assertSame('2', $merged->get('b'));
        self::assertSame('3', $merged->get('c'));
    }

    public function test_merge_is_immutable() : void
    {
        $resource1 = Resource::create(['a' => '1']);
        $resource2 = Resource::create(['b' => '2']);

        $merged = $resource1->merge($resource2);

        self::assertNotSame($resource1, $merged);
        self::assertNotSame($resource2, $merged);
        self::assertFalse($resource1->has('b'));
        self::assertFalse($resource2->has('a'));
    }

    public function test_merge_other_takes_precedence() : void
    {
        $resource1 = Resource::create(['key' => 'original']);
        $resource2 = Resource::create(['key' => 'override']);

        $merged = $resource1->merge($resource2);

        self::assertSame('override', $merged->get('key'));
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $original = Resource::create([
            'service.name' => 'test-service',
            'service.version' => '2.0.0',
            'host.name' => 'localhost',
            'process.pid' => 1234,
        ]);

        $normalized = $original->normalize();
        $restored = Resource::fromArray($normalized);

        self::assertSame($original->all(), $restored->all());
        self::assertSame($original->count(), $restored->count());
    }

    public function test_normalize_returns_array_representation() : void
    {
        $resource = Resource::create([
            'service.name' => 'my-service',
            'count' => 42,
            'enabled' => true,
        ]);

        $normalized = $resource->normalize();

        self::assertEquals([
            'attributes' => [
                'service.name' => 'my-service',
                'count' => 42,
                'enabled' => true,
            ],
        ], $normalized);
    }

    public function test_resource_is_immutable() : void
    {
        $original = Resource::create(['key' => 'value']);

        $withAdded = $original->with('new', 'entry');
        $merged = $original->merge(Resource::create(['other' => 'data']));

        self::assertNotSame($original, $withAdded);
        self::assertNotSame($original, $merged);
        self::assertSame(1, $original->count());
    }

    public function test_with_adds_attribute() : void
    {
        $resource = Resource::create();
        $newResource = $resource->with('key', 'value');

        self::assertFalse($resource->has('key'));
        self::assertTrue($newResource->has('key'));
        self::assertSame('value', $newResource->get('key'));
    }

    public function test_with_replaces_existing_attribute() : void
    {
        $resource = Resource::create(['key' => 'old']);
        $newResource = $resource->with('key', 'new');

        self::assertSame('old', $resource->get('key'));
        self::assertSame('new', $newResource->get('key'));
    }

    public function test_with_supports_various_types() : void
    {
        $resource = Resource::create()
            ->with('string', 'value')
            ->with('int', 42)
            ->with('float', 3.14)
            ->with('bool', true)
            ->with('array', ['a', 'b', 'c']);

        self::assertSame('value', $resource->get('string'));
        self::assertSame(42, $resource->get('int'));
        self::assertSame(3.14, $resource->get('float'));
        self::assertTrue($resource->get('bool'));
        self::assertSame(['a', 'b', 'c'], $resource->get('array'));
    }
}
