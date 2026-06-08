<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeRule;
use Flow\Telemetry\Filter\Compilation;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Filter\NotCompilable;
use Flow\Telemetry\Tests\Mother\TempDir;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_rule;

final class AttributeRuleTest extends TestCase
{
    public function test_compiled_matcher_matches_interpreted_result(): void
    {
        $rules = [
            attribute_rule('http.route', MatchMode::EQUAL, '/health'),
            attribute_rule('status', MatchMode::NOT_EQUAL, 200),
            attribute_rule('duration', MatchMode::GREATER_THAN, 100),
            attribute_rule('duration', MatchMode::LESS_THAN_EQUAL, 100),
            attribute_rule('name', MatchMode::STARTS_WITH, 'app.'),
            attribute_rule('name', MatchMode::ENDS_WITH, '.worker'),
            attribute_rule('ua', MatchMode::CONTAINS, 'bot', false),
            attribute_rule('email', MatchMode::REGEXP, '/@example\.com$/'),
            attribute_rule(['nested', 'value'], MatchMode::EQUAL, 'deep'),
        ];

        $attributeSets = [
            Attributes::create([]),
            Attributes::create(['http.route' => '/health', 'status' => 500, 'duration' => 150]),
            Attributes::create(['name' => 'app.report.worker', 'ua' => 'GoogleBot', 'email' => 'a@example.com']),
            Attributes::create(['nested' => ['value' => 'deep'], 'duration' => 100]),
        ];

        $tmp = TempDir::create();

        try {
            foreach ($rules as $rule) {
                // exclude (default) => shouldDrop() == compiled match; matches() is the interpreted path
                $compiled = attribute_filter($rule, cacheDir: $tmp->path());

                foreach ($attributeSets as $attributes) {
                    static::assertSame(
                        $rule->matches($attributes),
                        $compiled->shouldDrop($attributes),
                        'Compiled matcher diverged from interpreted matches() for '
                            . $rule->compile(new Compilation('$a', '$__v')),
                    );
                }
            }
        } finally {
            $tmp->remove();
        }
    }

    public function test_compile_throws_not_compilable_for_datetime_expected(): void
    {
        $this->expectException(NotCompilable::class);

        attribute_rule('t', MatchMode::GREATER_THAN, new DateTimeImmutable('2020-01-01'))->compile(new Compilation(
            '$a',
            '$__v',
        ));
    }

    public function test_matches_descends_into_nested_path(): void
    {
        static::assertTrue(attribute_rule(['user', 'id'], MatchMode::EQUAL, 5)->matches(Attributes::create(['user' => [
            'id' => 5,
        ]])));
    }

    public function test_matches_missing_attribute_does_not_match(): void
    {
        static::assertFalse(attribute_rule('absent', MatchMode::EQUAL, 'x')->matches(Attributes::create([])));
    }

    public function test_string_path_is_normalized_to_single_segment(): void
    {
        static::assertTrue(attribute_rule('http.route', MatchMode::EQUAL, '/health')->matches(Attributes::create([
            'http.route' => '/health',
        ])));
    }

    public function test_throws_on_empty_string_path(): void
    {
        $this->expectException(InvalidArgumentException::class);

        attribute_rule('', MatchMode::EQUAL, 'x');
    }

    public function test_throws_on_empty_path_array(): void
    {
        $this->expectException(InvalidArgumentException::class);

        attribute_rule([], MatchMode::EQUAL, 'x');
    }

    public function test_throws_on_invalid_regexp(): void
    {
        $this->expectException(InvalidArgumentException::class);

        attribute_rule('x', MatchMode::REGEXP, '/(unclosed');
    }

    public function test_throws_on_empty_path_segment(): void
    {
        $this->expectException(InvalidArgumentException::class);

        attribute_rule(['ok', ''], MatchMode::EQUAL, 'x');
    }

    /**
     * @return iterable<string, array{MatchMode}>
     */
    public static function patternModes(): iterable
    {
        yield 'regexp' => [MatchMode::REGEXP];
        yield 'starts_with' => [MatchMode::STARTS_WITH];
        yield 'ends_with' => [MatchMode::ENDS_WITH];
        yield 'contains' => [MatchMode::CONTAINS];
    }

    #[DataProvider('patternModes')]
    public function test_throws_when_pattern_mode_gets_non_string_expected(MatchMode $mode): void
    {
        $this->expectException(InvalidArgumentException::class);

        attribute_rule('x', $mode, 5);
    }

    /**
     * @return iterable<string, array{AttributeRule, string}>
     */
    public static function compiledExpressionProvider(): iterable
    {
        yield 'equal' => [
            attribute_rule('x', MatchMode::EQUAL, 'y'),
            '(($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === \'y\')',
        ];

        yield 'not_equal' => [
            attribute_rule('x', MatchMode::NOT_EQUAL, 'y'),
            '(($__m0 = ($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING)) !== \Flow\Telemetry\Filter\AttributeMatch::MISSING && $__m0 !== \'y\')',
        ];

        yield 'greater_than' => [
            attribute_rule('x', MatchMode::GREATER_THAN, 5),
            '(($__m0 = ($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING)) !== \Flow\Telemetry\Filter\AttributeMatch::MISSING && (is_scalar($__m0) || $__m0 instanceof \DateTimeInterface) && $__m0 > 5)',
        ];

        yield 'less_than_equal' => [
            attribute_rule('x', MatchMode::LESS_THAN_EQUAL, 5),
            '(($__m0 = ($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING)) !== \Flow\Telemetry\Filter\AttributeMatch::MISSING && (is_scalar($__m0) || $__m0 instanceof \DateTimeInterface) && $__m0 <= 5)',
        ];

        yield 'regexp' => [
            attribute_rule('x', MatchMode::REGEXP, '/a/'),
            '(($__m0 = \Flow\Telemetry\Filter\AttributeMatch::stringForm(($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING))) !== null && preg_match(\'/a/\', $__m0) === 1)',
        ];

        yield 'starts_with case-sensitive' => [
            attribute_rule('x', MatchMode::STARTS_WITH, 'pre'),
            '(($__m0 = \Flow\Telemetry\Filter\AttributeMatch::stringForm(($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING))) !== null && str_starts_with($__m0, \'pre\'))',
        ];

        yield 'contains case-insensitive' => [
            attribute_rule('x', MatchMode::CONTAINS, 'Bot', false),
            '(($__m0 = \Flow\Telemetry\Filter\AttributeMatch::stringForm(($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING))) !== null && str_contains(mb_strtolower($__m0), \'bot\'))',
        ];
    }

    #[DataProvider('compiledExpressionProvider')]
    public function test_compile_produces_expected_php_expression(AttributeRule $rule, string $expected): void
    {
        static::assertSame($expected, $rule->compile(new Compilation('$a', '$__v')));
    }

    public function test_compile_nested_path_descends_via_resolve(): void
    {
        static::assertSame(
            "(\\Flow\\Telemetry\\Filter\\AttributeMatch::resolve(\$a, array (\n  0 => 'a',\n  1 => 'b',\n)) === 'y')",
            attribute_rule(['a', 'b'], MatchMode::EQUAL, 'y')->compile(new Compilation('$a', '$__v')),
        );
    }
}
