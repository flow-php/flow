<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeMatch;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class AttributeMatchTest extends TestCase
{
    public function test_contains_empty_needle_always_matches(): void
    {
        static::assertTrue(AttributeMatch::contains('anything', '', true));
    }

    public function test_contains_is_case_insensitive_when_flag_disabled(): void
    {
        static::assertTrue(AttributeMatch::contains('GoogleBot/2.1', 'bot', false));
        static::assertFalse(AttributeMatch::contains('GoogleBot/2.1', 'bot', true));
    }

    public function test_equal_is_strict(): void
    {
        static::assertTrue(AttributeMatch::equal(5, 5));
        static::assertFalse(AttributeMatch::equal(5, '5'));
    }

    public function test_equal_on_missing_value_is_false(): void
    {
        static::assertFalse(AttributeMatch::equal(AttributeMatch::MISSING, 5));
    }

    public function test_ends_with_respects_case_flag(): void
    {
        static::assertTrue(AttributeMatch::endsWith('order.Created', 'created', false));
        static::assertFalse(AttributeMatch::endsWith('order.Created', 'created', true));
        static::assertTrue(AttributeMatch::endsWith('order.created', 'created', true));
    }

    #[TestWith([3, 5, true])]
    #[TestWith([5, 5, false])]
    #[TestWith([7, 5, false])]
    public function test_less_than(int $value, int $expected, bool $matches): void
    {
        static::assertSame($matches, AttributeMatch::lessThan($value, $expected));
    }

    #[TestWith([7, 5, true])]
    #[TestWith([5, 5, false])]
    #[TestWith([3, 5, false])]
    public function test_greater_than(int $value, int $expected, bool $matches): void
    {
        static::assertSame($matches, AttributeMatch::greaterThan($value, $expected));
    }

    public function test_greater_than_equal_and_less_than_equal_boundaries(): void
    {
        static::assertTrue(AttributeMatch::greaterThanEqual(5, 5));
        static::assertTrue(AttributeMatch::lessThanEqual(5, 5));
        static::assertFalse(AttributeMatch::greaterThanEqual(4, 5));
        static::assertFalse(AttributeMatch::lessThanEqual(6, 5));
    }

    public function test_comparisons_on_missing_value_are_false(): void
    {
        static::assertFalse(AttributeMatch::greaterThan(AttributeMatch::MISSING, 5));
        static::assertFalse(AttributeMatch::greaterThanEqual(AttributeMatch::MISSING, 5));
        static::assertFalse(AttributeMatch::lessThan(AttributeMatch::MISSING, 5));
        static::assertFalse(AttributeMatch::lessThanEqual(AttributeMatch::MISSING, 5));
    }

    public function test_not_equal_matches_different_value(): void
    {
        static::assertTrue(AttributeMatch::notEqual('a', 'b'));
        static::assertFalse(AttributeMatch::notEqual('a', 'a'));
    }

    public function test_not_equal_on_missing_value_is_false(): void
    {
        static::assertFalse(AttributeMatch::notEqual(AttributeMatch::MISSING, 'b'));
    }

    public function test_regexp_matches_and_rejects(): void
    {
        static::assertTrue(AttributeMatch::regexp('a@example.com', '/@example\.com$/'));
        static::assertFalse(AttributeMatch::regexp('a@other.com', '/@example\.com$/'));
    }

    public function test_regexp_on_null_string_is_false(): void
    {
        static::assertFalse(AttributeMatch::regexp(null, '/.*/'));
    }

    public function test_resolve_descends_into_nested_arrays(): void
    {
        static::assertSame(5, AttributeMatch::resolve(Attributes::create(['user' => ['id' => 5]]), ['user', 'id']));
    }

    public function test_resolve_returns_missing_for_absent_top_level_key(): void
    {
        static::assertSame(AttributeMatch::MISSING, AttributeMatch::resolve(Attributes::create([]), ['nope']));
    }

    public function test_resolve_returns_missing_for_absent_nested_segment(): void
    {
        static::assertSame(AttributeMatch::MISSING, AttributeMatch::resolve(Attributes::create(['user' => [
            'id' => 5,
        ]]), ['user', 'name']));
    }

    public function test_resolve_returns_missing_when_intermediate_is_not_an_array(): void
    {
        static::assertSame(AttributeMatch::MISSING, AttributeMatch::resolve(Attributes::create([
            'user' => 'scalar',
        ]), ['user', 'id']));
    }

    public function test_resolve_top_level_scalar(): void
    {
        static::assertSame('/health', AttributeMatch::resolve(Attributes::create([
            'http.route' => '/health',
        ]), ['http.route']));
    }

    public function test_starts_with_respects_case_flag(): void
    {
        static::assertTrue(AttributeMatch::startsWith('Vendor.Chatty', 'vendor.', false));
        static::assertFalse(AttributeMatch::startsWith('Vendor.Chatty', 'vendor.', true));
    }

    public function test_string_form_of_bool(): void
    {
        static::assertSame('true', AttributeMatch::stringForm(true));
        static::assertSame('false', AttributeMatch::stringForm(false));
    }

    public function test_string_form_of_datetime_is_iso8601(): void
    {
        static::assertSame(
            '2024-01-15T10:30:00+00:00',
            AttributeMatch::stringForm(new DateTimeImmutable('2024-01-15T10:30:00+00:00')),
        );
    }

    #[TestWith([5, '5'])]
    #[TestWith([1.5, '1.5'])]
    #[TestWith(['plain', 'plain'])]
    public function test_string_form_of_scalars(int|float|string $value, string $expected): void
    {
        static::assertSame($expected, AttributeMatch::stringForm($value));
    }

    public function test_string_form_of_unstringable_values_is_null(): void
    {
        static::assertNull(AttributeMatch::stringForm(['a', 'b']));
        static::assertNull(AttributeMatch::stringForm(new RuntimeException('boom')));
        static::assertNull(AttributeMatch::stringForm(AttributeMatch::MISSING));
    }

    public function test_substring_modes_on_null_string_are_false(): void
    {
        static::assertFalse(AttributeMatch::startsWith(null, 'x', true));
        static::assertFalse(AttributeMatch::endsWith(null, 'x', true));
        static::assertFalse(AttributeMatch::contains(null, 'x', true));
    }
}
