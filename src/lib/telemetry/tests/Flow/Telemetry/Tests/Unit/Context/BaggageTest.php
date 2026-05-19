<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Baggage;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function count;

final class BaggageTest extends TestCase
{
    public static function provideEntriesForCount(): Generator
    {
        yield 'empty' => [[], 0];
        yield 'single entry' => [['a' => '1'], 1];
        yield 'three entries' => [['a' => '1', 'b' => '2', 'c' => '3'], 3];
        yield 'five entries' => [['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5'], 5];
    }

    public static function provideInitialEntries(): Generator
    {
        yield 'empty' => [[]];
        yield 'single entry' => [['key' => 'value']];
        yield 'multiple entries' => [['user.id' => '12345', 'request.id' => 'abc-123']];
        yield 'dotted keys' => [['service.name' => 'test', 'host.name' => 'localhost']];
    }

    public static function provideKeyValuePairs(): Generator
    {
        yield 'simple key' => ['key', 'value'];
        yield 'dotted key' => ['user.id', '12345'];
        yield 'numeric value' => ['count', '42'];
        yield 'empty value' => ['empty', ''];
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideInitialEntries')]
    public function test_baggage_can_be_created_with_entries(array $entries): void
    {
        $baggage = new Baggage($entries);

        static::assertSame($entries, $baggage->all());
        static::assertSame(count($entries), $baggage->count());
        static::assertSame(count($entries) === 0, $baggage->isEmpty());
    }

    public function test_baggage_is_immutable(): void
    {
        $original = new Baggage(['key' => 'value']);

        $withAdded = $original->with('new', 'entry');
        $withRemoved = $original->without('key');

        static::assertNotSame($original, $withAdded);
        static::assertNotSame($original, $withRemoved);
        static::assertSame(1, $original->count());
        static::assertSame(2, $withAdded->count());
        static::assertSame(0, $withRemoved->count());
    }

    public function test_chained_operations(): void
    {
        $baggage = (new Baggage())
            ->with('a', '1')
            ->with('b', '2')
            ->with('c', '3')
            ->without('b');

        static::assertSame(2, $baggage->count());
        static::assertSame('1', $baggage->get('a'));
        static::assertNull($baggage->get('b'));
        static::assertSame('3', $baggage->get('c'));
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideEntriesForCount')]
    public function test_count_returns_correct_number(array $entries, int $expectedCount): void
    {
        $baggage = new Baggage($entries);

        static::assertSame($expectedCount, $baggage->count());
    }

    public function test_empty_baggage_has_no_entries(): void
    {
        $baggage = new Baggage();

        static::assertTrue($baggage->isEmpty());
        static::assertSame(0, $baggage->count());
        static::assertSame([], $baggage->all());
    }

    public function test_from_array_creates_baggage(): void
    {
        $entries = ['user.id' => '12345', 'request.id' => 'abc-123'];
        $baggage = Baggage::fromArray(['entries' => $entries]);

        static::assertSame($entries, $baggage->all());
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $baggage = new Baggage();

        static::assertNull($baggage->get('missing'));
    }

    #[DataProvider('provideKeyValuePairs')]
    public function test_get_returns_value_for_existing_key(string $key, string $value): void
    {
        $baggage = new Baggage([$key => $value]);

        static::assertSame($value, $baggage->get($key));
    }

    public function test_has_returns_false_for_missing_key(): void
    {
        $baggage = new Baggage();

        static::assertFalse($baggage->has('missing'));
    }

    #[DataProvider('provideKeyValuePairs')]
    public function test_has_returns_true_for_existing_key(string $key, string $value): void
    {
        $baggage = new Baggage([$key => $value]);

        static::assertTrue($baggage->has($key));
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $original = new Baggage(['a' => '1', 'b' => '2', 'c' => '3']);
        $normalized = $original->normalize();
        $restored = Baggage::fromArray($normalized);

        static::assertSame($original->all(), $restored->all());
    }

    public function test_normalize_returns_array_with_entries(): void
    {
        $entries = ['user.id' => '12345', 'request.id' => 'abc-123'];
        $baggage = new Baggage($entries);

        static::assertSame(['entries' => $entries], $baggage->normalize());
    }

    public function test_normalize_returns_empty_entries_for_empty_baggage(): void
    {
        $baggage = new Baggage();

        static::assertSame(['entries' => []], $baggage->normalize());
    }

    #[DataProvider('provideKeyValuePairs')]
    public function test_with_adds_entry(string $key, string $value): void
    {
        $baggage = new Baggage();
        $newBaggage = $baggage->with($key, $value);

        static::assertFalse($baggage->has($key));
        static::assertTrue($newBaggage->has($key));
        static::assertSame($value, $newBaggage->get($key));
    }

    public function test_with_replaces_existing_entry(): void
    {
        $baggage = new Baggage(['key' => 'old']);
        $newBaggage = $baggage->with('key', 'new');

        static::assertSame('old', $baggage->get('key'));
        static::assertSame('new', $newBaggage->get('key'));
    }

    public function test_without_non_existent_key_returns_same_values(): void
    {
        $baggage = new Baggage(['key' => 'value']);
        $newBaggage = $baggage->without('non_existent');

        static::assertSame(1, $newBaggage->count());
        static::assertSame('value', $newBaggage->get('key'));
    }

    public function test_without_removes_entry(): void
    {
        $baggage = new Baggage(['key1' => 'value1', 'key2' => 'value2']);
        $newBaggage = $baggage->without('key1');

        static::assertTrue($baggage->has('key1'));
        static::assertFalse($newBaggage->has('key1'));
        static::assertTrue($newBaggage->has('key2'));
    }
}
