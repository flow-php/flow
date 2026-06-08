<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\Matcher;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\not;

/**
 * The generated (compiled) matcher and the interpreted matcher are two
 * implementations of the same semantics. This test pins them together: for every
 * matcher shape, the compiled closure and {@see Matcher::matches()} must return
 * the same result on every attribute set. If they ever diverge, this fails.
 */
final class CompilationParityTest extends TestCase
{
    #[DataProvider('matchers')]
    public function test_compiled_result_equals_interpreted_result(Matcher $matcher): void
    {
        $tmp = TempDir::create();

        try {
            $compiled = attribute_filter($matcher, cacheDir: $tmp->path());

            foreach (self::attributeSets() as $label => $attributes) {
                // exclude (default) => shouldDrop() is exactly the compiled match result
                static::assertSame(
                    $matcher->matches($attributes),
                    $compiled->shouldDrop($attributes),
                    sprintf('compiled diverged from interpreted for attribute set "%s"', $label),
                );
            }
        } finally {
            $tmp->remove();
        }
    }

    /**
     * @return iterable<string, array{Matcher}>
     */
    public static function matchers(): iterable
    {
        foreach (['equal' => MatchMode::EQUAL, 'not_equal' => MatchMode::NOT_EQUAL] as $name => $mode) {
            foreach ([
                'str' => 'AB',
                'empty' => '',
                'int' => 5,
                'zero' => 0,
                'float' => 5.5,
                'true' => true,
                'false' => false,
            ] as $type => $expected) {
                yield "{$name}:{$type}" => [attribute_rule('k', $mode, $expected)];
            }
        }

        foreach ([
            'gt' => MatchMode::GREATER_THAN,
            'gte' => MatchMode::GREATER_THAN_EQUAL,
            'lt' => MatchMode::LESS_THAN,
            'lte' => MatchMode::LESS_THAN_EQUAL,
        ] as $name => $mode) {
            foreach (['int' => 5, 'zero' => 0, 'float' => 5.5, 'str' => 'm'] as $type => $expected) {
                yield "{$name}:{$type}" => [attribute_rule('k', $mode, $expected)];
            }
        }

        foreach ([
            'starts_with' => MatchMode::STARTS_WITH,
            'ends_with' => MatchMode::ENDS_WITH,
            'contains' => MatchMode::CONTAINS,
        ] as $name => $mode) {
            foreach (['cs:AB' => ['AB', true], 'ci:ab' => ['ab', false], 'empty' => ['', true]] as $type => [
                $needle,
                $cs,
            ]) {
                yield "{$name}:{$type}" => [attribute_rule('k', $mode, $needle, $cs)];
            }
        }

        yield 'regexp:anchored' => [attribute_rule('k', MatchMode::REGEXP, '/^a.c$/i')];
        yield 'regexp:contains' => [attribute_rule('k', MatchMode::REGEXP, '/x/')];

        yield 'nested:equal' => [attribute_rule(['p', 'q'], MatchMode::EQUAL, 'deep')];
        yield 'nested:not_equal' => [attribute_rule(['p', 'q'], MatchMode::NOT_EQUAL, 'deep')];
        yield 'nested:gt' => [attribute_rule(['p', 'q'], MatchMode::GREATER_THAN, 10)];
        yield 'nested:contains' => [attribute_rule(['p', 'q'], MatchMode::CONTAINS, 'ee')];

        yield 'all' => [all(
            attribute_rule('k', MatchMode::EQUAL, 'AB'),
            attribute_rule('k', MatchMode::STARTS_WITH, 'A'),
        )];
        yield 'any' => [any(
            attribute_rule('k', MatchMode::GREATER_THAN, 4),
            attribute_rule('k', MatchMode::CONTAINS, 'B', false),
        )];
        yield 'not' => [not(attribute_rule('k', MatchMode::EQUAL, 'AB'))];
        yield 'deep' => [all(
            not(attribute_rule('k', MatchMode::EQUAL, 'skip')),
            any(
                attribute_rule('k', MatchMode::GREATER_THAN_EQUAL, 5),
                attribute_rule('k', MatchMode::REGEXP, '/^a/i'),
                attribute_rule(['p', 'q'], MatchMode::EQUAL, 'deep'),
            ),
        )];
    }

    /**
     * @return array<string, Attributes>
     */
    public static function attributeSets(): array
    {
        return [
            'absent' => Attributes::create([]),
            'str-AB' => Attributes::create(['k' => 'AB']),
            'str-abc' => Attributes::create(['k' => 'aBc']),
            'str-mixed' => Attributes::create(['k' => 'xABy']),
            'str-empty' => Attributes::create(['k' => '']),
            'str-m' => Attributes::create(['k' => 'm']),
            'str-z' => Attributes::create(['k' => 'z']),
            'int-5' => Attributes::create(['k' => 5]),
            'int-0' => Attributes::create(['k' => 0]),
            'int-9' => Attributes::create(['k' => 9]),
            'float' => Attributes::create(['k' => 5.5]),
            'bool-true' => Attributes::create(['k' => true]),
            'bool-false' => Attributes::create(['k' => false]),
            'array' => Attributes::create(['k' => ['nested']]),
            'nested-deep' => Attributes::create(['p' => ['q' => 'deep'], 'k' => 'AB']),
            'nested-num' => Attributes::create(['p' => ['q' => 50]]),
            'nested-null' => Attributes::create(['p' => ['q' => null]]),
            'nested-scalar-parent' => Attributes::create(['p' => 'scalar']),
            'nested-absent-child' => Attributes::create(['p' => []]),
        ];
    }
}
