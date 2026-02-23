<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\{AttributeLimitsEnforcer, Attributes};
use PHPUnit\Framework\TestCase;

final class AttributeLimitsEnforcerTest extends TestCase
{
    public function test_applies_both_count_and_length_limits() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'key1' => 'short',
            'key2' => 'this-is-a-long-value',
            'key3' => 'another-long-value',
            'key4' => 'dropped',
        ]);

        $result = $enforcer->enforce($attributes, 3, 10);

        self::assertSame(3, $result->attributes->count());
        self::assertSame(1, $result->droppedAttributeCount);
        self::assertSame('short', $result->attributes->get('key1'));
        self::assertSame('this-is-a-', $result->attributes->get('key2'));
        self::assertSame('another-lo', $result->attributes->get('key3'));
    }

    public function test_does_not_drop_attributes_when_exactly_at_limit() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
        ]);

        $result = $enforcer->enforce($attributes, 3, null);

        self::assertSame(3, $result->attributes->count());
        self::assertSame(0, $result->droppedAttributeCount);
    }

    public function test_does_not_drop_attributes_when_under_limit() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
        ]);

        $result = $enforcer->enforce($attributes, 10, null);

        self::assertSame(3, $result->attributes->count());
        self::assertSame(0, $result->droppedAttributeCount);
        self::assertSame('value1', $result->attributes->get('key1'));
        self::assertSame('value2', $result->attributes->get('key2'));
        self::assertSame('value3', $result->attributes->get('key3'));
    }

    public function test_does_not_modify_non_string_values() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'int' => 123456789012345,
            'float' => 123456.789012345,
            'bool' => true,
        ]);

        $result = $enforcer->enforce($attributes, 10, 5);

        self::assertSame(123456789012345, $result->attributes->get('int'));
        self::assertSame(123456.789012345, $result->attributes->get('float'));
        self::assertTrue($result->attributes->get('bool'));
    }

    public function test_drops_attributes_exceeding_count_limit() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
            'key4' => 'value4',
            'key5' => 'value5',
        ]);

        $result = $enforcer->enforce($attributes, 3, null);

        self::assertSame(3, $result->attributes->count());
        self::assertSame(2, $result->droppedAttributeCount);
        self::assertSame('value1', $result->attributes->get('key1'));
        self::assertSame('value2', $result->attributes->get('key2'));
        self::assertSame('value3', $result->attributes->get('key3'));
        self::assertNull($result->attributes->get('key4'));
        self::assertNull($result->attributes->get('key5'));
    }

    public function test_handles_empty_attributes() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::empty();

        $result = $enforcer->enforce($attributes, 10, 10);

        self::assertSame(0, $result->attributes->count());
        self::assertSame(0, $result->droppedAttributeCount);
    }

    public function test_null_value_length_limit_does_not_truncate() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'long' => \str_repeat('a', 1000),
        ]);

        $result = $enforcer->enforce($attributes, 10, null);

        $value = $result->attributes->get('long');
        self::assertIsString($value);
        self::assertSame(1000, \strlen($value));
    }

    public function test_preserves_array_with_non_string_values() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'mixed' => [1, 2.5, true, 'truncate-this-string'],
        ]);

        $result = $enforcer->enforce($attributes, 10, 10);

        self::assertSame([1, 2.5, true, 'truncate-t'], $result->attributes->get('mixed'));
    }

    public function test_truncates_array_string_values() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'tags' => ['short', 'this-is-a-very-long-tag-value'],
        ]);

        $result = $enforcer->enforce($attributes, 10, 10);

        self::assertSame(['short', 'this-is-a-'], $result->attributes->get('tags'));
    }

    public function test_truncates_string_values_exceeding_length_limit() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'short' => 'abc',
            'long' => 'abcdefghijklmnopqrstuvwxyz',
        ]);

        $result = $enforcer->enforce($attributes, 10, 10);

        self::assertSame('abc', $result->attributes->get('short'));
        self::assertSame('abcdefghij', $result->attributes->get('long'));
        self::assertSame(0, $result->droppedAttributeCount);
    }

    public function test_truncates_string_values_using_utf8_safe_substring() : void
    {
        $enforcer = new AttributeLimitsEnforcer();
        $attributes = Attributes::create([
            'unicode' => 'Zażółć gęślą jaźń',
        ]);

        $result = $enforcer->enforce($attributes, 10, 10);

        self::assertSame('Zażółć gęś', $result->attributes->get('unicode'));
    }
}
