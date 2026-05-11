<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\TraceState;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TraceStateTest extends TestCase
{
    public static function provideInvalidKeys(): \Generator
    {
        yield 'starts with digit' => ['1vendor', 'Invalid TraceState key'];
        yield 'contains uppercase' => ['VENDOR', 'Invalid TraceState key'];
        yield 'contains invalid char' => ['vendor!key', 'Invalid TraceState key'];
        yield 'empty key' => ['', 'TraceState key cannot be empty'];
        yield 'too long simple key' => [\str_repeat('a', 257), 'Invalid TraceState key'];
        yield 'invalid multi-tenant format' => ['vendor@', 'Invalid TraceState key'];
    }

    public static function provideValidKeys(): \Generator
    {
        yield 'simple lowercase' => ['vendor'];
        yield 'with hyphen' => ['vendor-key'];
        yield 'with underscore' => ['vendor_key'];
        yield 'with asterisk' => ['vendor*key'];
        yield 'with slash' => ['vendor/key'];
        yield 'multi-tenant format' => ['fw@vendor'];
        yield 'digit after first char' => ['vendor1'];
    }

    public static function provideValidTraceStateStrings(): \Generator
    {
        yield 'single entry' => ['congo=t61rcWkgMzE', ['congo' => 't61rcWkgMzE']];
        yield 'multiple entries' => [
            'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE',
            ['rojo' => '00f067aa0ba902b7', 'congo' => 't61rcWkgMzE'],
        ];
        yield 'with spaces after commas' => [
            'rojo=00f067aa0ba902b7, congo=t61rcWkgMzE',
            ['rojo' => '00f067aa0ba902b7', 'congo' => 't61rcWkgMzE'],
        ];
        yield 'empty string' => ['', []];
        yield 'whitespace only' => ['   ', []];
    }

    public function test_all_returns_entries_in_order(): void
    {
        $state = TraceState::empty()->with('first', '1')->with('second', '2')->with('third', '3');

        $all = $state->all();
        $keys = \array_keys($all);

        static::assertSame(['third', 'second', 'first'], $keys);
    }

    public function test_empty_creates_empty_state(): void
    {
        $state = TraceState::empty();

        static::assertTrue($state->isEmpty());
        static::assertSame([], $state->all());
    }

    /**
     * @param array<string, string> $expected
     */
    #[DataProvider('provideValidTraceStateStrings')]
    public function test_from_string_parses_correctly(string $input, array $expected): void
    {
        $state = TraceState::fromString($input);

        static::assertSame($expected, $state->all());
    }

    public function test_from_string_throws_on_invalid_entries(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid tracestate entry');

        TraceState::fromString('valid=value,invalid,also=valid');
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $state = TraceState::empty();

        static::assertNull($state->get('nonexistent'));
    }

    public function test_get_returns_value_for_existing_key(): void
    {
        $state = TraceState::empty()->with('key', 'value');

        static::assertSame('value', $state->get('key'));
    }

    public function test_is_empty_returns_false_for_non_empty_state(): void
    {
        $state = TraceState::empty()->with('key', 'value');

        static::assertFalse($state->isEmpty());
    }

    public function test_is_empty_returns_true_for_empty_state(): void
    {
        static::assertTrue(TraceState::empty()->isEmpty());
    }

    public function test_round_trip_string_parsing(): void
    {
        $original = 'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE';
        $state = TraceState::fromString($original);
        $result = $state->toString();

        static::assertSame($original, $result);
    }

    public function test_stringable_interface(): void
    {
        $state = TraceState::empty()->with('key', 'value');

        static::assertSame($state->toString(), (string) $state);
    }

    public function test_to_string_formats_correctly(): void
    {
        $state = TraceState::empty()->with('rojo', '00f067aa0ba902b7')->with('congo', 't61rcWkgMzE');

        $string = $state->toString();

        static::assertStringContainsString('rojo=00f067aa0ba902b7', $string);
        static::assertStringContainsString('congo=t61rcWkgMzE', $string);
    }

    public function test_to_string_returns_empty_for_empty_state(): void
    {
        $state = TraceState::empty();

        static::assertSame('', $state->toString());
    }

    #[DataProvider('provideValidKeys')]
    public function test_with_accepts_valid_keys(string $key): void
    {
        $state = TraceState::empty()->with($key, 'value');

        static::assertSame('value', $state->get($key));
    }

    public function test_with_moves_updated_key_to_front(): void
    {
        $state = TraceState::empty()->with('first', '1')->with('second', '2')->with('first', 'updated');

        $keys = \array_keys($state->all());

        static::assertSame('first', $keys[0]);
    }

    public function test_with_respects_max_entries_limit(): void
    {
        $state = TraceState::empty();

        for ($i = 0; $i < 32; $i++) {
            $state = $state->with("key{$i}", "value{$i}");
        }

        static::assertCount(32, $state->all());

        $state = $state->with('key32', 'value32');
        static::assertCount(32, $state->all());
        static::assertSame('value32', $state->get('key32'));
    }

    public function test_with_returns_new_instance(): void
    {
        $original = TraceState::empty();
        $modified = $original->with('key', 'value');

        static::assertNotSame($original, $modified);
        static::assertTrue($original->isEmpty());
        static::assertFalse($modified->isEmpty());
    }

    #[DataProvider('provideInvalidKeys')]
    public function test_with_throws_on_invalid_key(string $key, string $expectedMessage): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        TraceState::empty()->with($key, 'value');
    }

    public function test_with_updates_existing_key(): void
    {
        $state = TraceState::empty()->with('key', 'value1')->with('key', 'value2');

        static::assertSame('value2', $state->get('key'));
        static::assertCount(1, $state->all());
    }

    public function test_without_is_noop_for_missing_key(): void
    {
        $state = TraceState::empty()->with('key', 'value');
        $modified = $state->without('nonexistent');

        static::assertSame($state->all(), $modified->all());
    }

    public function test_without_removes_existing_key(): void
    {
        $state = TraceState::empty()->with('key1', 'value1')->with('key2', 'value2')->without('key1');

        static::assertNull($state->get('key1'));
        static::assertSame('value2', $state->get('key2'));
    }

    public function test_without_returns_new_instance(): void
    {
        $original = TraceState::empty()->with('key', 'value');
        $modified = $original->without('key');

        static::assertNotSame($original, $modified);
        static::assertSame('value', $original->get('key'));
        static::assertNull($modified->get('key'));
    }
}
