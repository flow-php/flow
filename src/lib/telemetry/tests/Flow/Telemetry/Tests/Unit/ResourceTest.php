<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Resource;
use PHPUnit\Framework\TestCase;

final class ResourceTest extends TestCase
{
    public function test_all_returns_all_attributes(): void
    {
        $attributes = [
            'a' => '1',
            'b' => 2,
            'c' => true,
        ];
        $resource = Resource::create($attributes);

        static::assertSame($attributes, $resource->all());
    }

    public function test_chained_operations(): void
    {
        $resource = Resource::create()
            ->with('service.name', 'my-service')
            ->with('service.version', '1.0.0')
            ->with('host.name', 'localhost');

        static::assertSame(3, $resource->count());
        static::assertSame('my-service', $resource->get('service.name'));
        static::assertSame('1.0.0', $resource->get('service.version'));
        static::assertSame('localhost', $resource->get('host.name'));
    }

    public function test_count_returns_correct_number(): void
    {
        $resource = Resource::create(['a' => '1', 'b' => '2', 'c' => '3']);

        static::assertSame(3, $resource->count());
    }

    public function test_create_empty_resource(): void
    {
        $resource = Resource::create();

        static::assertTrue($resource->isEmpty());
        static::assertSame(0, $resource->count());
    }

    public function test_create_with_attributes(): void
    {
        $attributes = [
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ];

        $resource = Resource::create($attributes);

        static::assertSame('my-service', $resource->get('service.name'));
        static::assertSame('1.0.0', $resource->get('service.version'));
    }

    public function test_empty_factory_method(): void
    {
        $resource = Resource::empty();

        static::assertTrue($resource->isEmpty());
        static::assertSame([], $resource->all());
    }

    public function test_from_array_creates_resource(): void
    {
        $data = [
            'attributes' => [
                'service.name' => 'my-service',
                'service.version' => '1.0.0',
            ],
        ];

        $resource = Resource::fromArray($data);

        static::assertSame('my-service', $resource->get('service.name'));
        static::assertSame('1.0.0', $resource->get('service.version'));
        static::assertSame(2, $resource->count());
    }

    public function test_from_array_with_empty_attributes(): void
    {
        $data = ['attributes' => []];

        $resource = Resource::fromArray($data);

        static::assertTrue($resource->isEmpty());
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $resource = Resource::create();

        static::assertNull($resource->get('missing'));
    }

    public function test_has_returns_false_for_missing_key(): void
    {
        $resource = Resource::create();

        static::assertFalse($resource->has('missing'));
    }

    public function test_has_returns_true_for_existing_key(): void
    {
        $resource = Resource::create(['key' => 'value']);

        static::assertTrue($resource->has('key'));
    }

    public function test_is_empty_returns_false_for_non_empty_resource(): void
    {
        $resource = Resource::create(['key' => 'value']);

        static::assertFalse($resource->isEmpty());
    }

    public function test_is_empty_returns_true_for_empty_resource(): void
    {
        $resource = Resource::empty();

        static::assertTrue($resource->isEmpty());
    }

    public function test_merge_combines_resources(): void
    {
        $resource1 = Resource::create(['a' => '1', 'b' => '2']);
        $resource2 = Resource::create(['c' => '3']);

        $merged = $resource1->merge($resource2);

        static::assertSame('1', $merged->get('a'));
        static::assertSame('2', $merged->get('b'));
        static::assertSame('3', $merged->get('c'));
    }

    public function test_merge_is_immutable(): void
    {
        $resource1 = Resource::create(['a' => '1']);
        $resource2 = Resource::create(['b' => '2']);

        $merged = $resource1->merge($resource2);

        static::assertNotSame($resource1, $merged);
        static::assertNotSame($resource2, $merged);
        static::assertFalse($resource1->has('b'));
        static::assertFalse($resource2->has('a'));
    }

    public function test_merge_other_takes_precedence(): void
    {
        $resource1 = Resource::create(['key' => 'original']);
        $resource2 = Resource::create(['key' => 'override']);

        $merged = $resource1->merge($resource2);

        static::assertSame('override', $merged->get('key'));
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $original = Resource::create([
            'service.name' => 'test-service',
            'service.version' => '2.0.0',
            'host.name' => 'localhost',
            'process.pid' => 1234,
        ]);

        $normalized = $original->normalize();
        $restored = Resource::fromArray($normalized);

        static::assertSame($original->all(), $restored->all());
        static::assertSame($original->count(), $restored->count());
    }

    public function test_normalize_returns_array_representation(): void
    {
        $resource = Resource::create([
            'service.name' => 'my-service',
            'count' => 42,
            'enabled' => true,
        ]);

        $normalized = $resource->normalize();

        static::assertEquals(
            [
                'attributes' => [
                    'service.name' => 'my-service',
                    'count' => 42,
                    'enabled' => true,
                ],
            ],
            $normalized,
        );
    }

    public function test_resource_is_immutable(): void
    {
        $original = Resource::create(['key' => 'value']);

        $withAdded = $original->with('new', 'entry');
        $merged = $original->merge(Resource::create(['other' => 'data']));

        static::assertNotSame($original, $withAdded);
        static::assertNotSame($original, $merged);
        static::assertSame(1, $original->count());
    }

    public function test_with_adds_attribute(): void
    {
        $resource = Resource::create();
        $newResource = $resource->with('key', 'value');

        static::assertFalse($resource->has('key'));
        static::assertTrue($newResource->has('key'));
        static::assertSame('value', $newResource->get('key'));
    }

    public function test_with_replaces_existing_attribute(): void
    {
        $resource = Resource::create(['key' => 'old']);
        $newResource = $resource->with('key', 'new');

        static::assertSame('old', $resource->get('key'));
        static::assertSame('new', $newResource->get('key'));
    }

    public function test_with_supports_various_types(): void
    {
        $resource = Resource::create()
            ->with('string', 'value')
            ->with('int', 42)
            ->with('float', 3.14)
            ->with('bool', true)
            ->with('array', ['a', 'b', 'c']);

        static::assertSame('value', $resource->get('string'));
        static::assertSame(42, $resource->get('int'));
        static::assertSame(3.14, $resource->get('float'));
        static::assertTrue($resource->get('bool'));
        static::assertSame(['a', 'b', 'c'], $resource->get('array'));
    }
}
