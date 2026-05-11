<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use PHPUnit\Framework\TestCase;

final class InstrumentationScopeTest extends TestCase
{
    public function test_constructor_with_all_parameters(): void
    {
        $attributes = Attributes::create(['key' => 'value']);
        $scope = new InstrumentationScope(
            'my-library',
            '1.2.3',
            'https://opentelemetry.io/schemas/1.17.0',
            $attributes,
        );

        static::assertSame('my-library', $scope->name);
        static::assertSame('1.2.3', $scope->version);
        static::assertSame('https://opentelemetry.io/schemas/1.17.0', $scope->schemaUrl);
        static::assertSame(['key' => 'value'], $scope->attributes->normalize());
    }

    public function test_constructor_with_custom_version(): void
    {
        $scope = new InstrumentationScope('my-library', '2.0.0');

        static::assertSame('2.0.0', $scope->version);
    }

    public function test_constructor_with_minimum_parameters(): void
    {
        $scope = new InstrumentationScope('my-library');

        static::assertSame('my-library', $scope->name);
        static::assertSame('unknown', $scope->version);
        static::assertNull($scope->schemaUrl);
        static::assertTrue($scope->attributes->isEmpty());
    }

    public function test_from_array_creates_scope(): void
    {
        $data = [
            'name' => 'test-library',
            'version' => '1.2.3',
            'schemaUrl' => 'https://schema.url',
            'attributes' => ['key' => 'value'],
        ];

        $scope = InstrumentationScope::fromArray($data);

        static::assertSame('test-library', $scope->name);
        static::assertSame('1.2.3', $scope->version);
        static::assertSame('https://schema.url', $scope->schemaUrl);
        static::assertSame(['key' => 'value'], $scope->attributes->normalize());
    }

    public function test_from_array_with_missing_optional_fields(): void
    {
        $data = [
            'name' => 'test-library',
        ];

        $scope = InstrumentationScope::fromArray($data);

        static::assertSame('test-library', $scope->name);
        static::assertSame('unknown', $scope->version);
        static::assertNull($scope->schemaUrl);
        static::assertTrue($scope->attributes->isEmpty());
    }

    public function test_normalize_returns_array_representation(): void
    {
        $scope = new InstrumentationScope('my-library', '1.0.0', 'https://schema.url', Attributes::create([
            'key' => 'value',
        ]));

        static::assertSame(
            [
                'name' => 'my-library',
                'version' => '1.0.0',
                'schemaUrl' => 'https://schema.url',
                'attributes' => ['key' => 'value'],
            ],
            $scope->normalize(),
        );
    }

    public function test_round_trip_normalize_from_array(): void
    {
        $original = new InstrumentationScope('test-library', '1.0.0', 'https://schema.url', Attributes::create([
            'key' => 'value',
        ]));

        $normalized = $original->normalize();
        $restored = InstrumentationScope::fromArray($normalized);

        static::assertSame($original->name, $restored->name);
        static::assertSame($original->version, $restored->version);
        static::assertSame($original->schemaUrl, $restored->schemaUrl);
        static::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
    }
}
