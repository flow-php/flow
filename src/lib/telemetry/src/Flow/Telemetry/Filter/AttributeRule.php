<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use DateTimeInterface;
use Flow\Telemetry\Attributes;
use InvalidArgumentException;

use function array_values;
use function in_array;
use function is_string;
use function sprintf;
use function var_export;

/**
 * A single attribute-matching rule: a path into an attribute set, a comparison
 * {@see MatchMode}, and an expected value. The leaf {@see Matcher}.
 */
final readonly class AttributeRule implements CompilableMatcher
{
    private const PATTERN_MODES = [
        MatchMode::REGEXP,
        MatchMode::STARTS_WITH,
        MatchMode::ENDS_WITH,
        MatchMode::CONTAINS,
    ];

    /**
     * @var non-empty-list<string>
     */
    private array $segments;

    /**
     * @param array<string>|string $path attribute path: a top-level key, or segments descending into nested arrays
     * @param bool $caseSensitive applies to the substring modes only ({@see MatchMode::STARTS_WITH}, {@see MatchMode::ENDS_WITH}, {@see MatchMode::CONTAINS})
     */
    public function __construct(
        array|string $path,
        private MatchMode $mode,
        private string|int|float|bool|DateTimeInterface $expected,
        private bool $caseSensitive = true,
    ) {
        $segments = is_string($path) ? [$path] : array_values($path);

        if ($segments === []) {
            throw new InvalidArgumentException('Attribute rule path must contain at least one segment');
        }

        foreach ($segments as $segment) {
            if ($segment === '') {
                throw new InvalidArgumentException('Attribute rule path segments must not be empty');
            }
        }

        if (in_array($mode, self::PATTERN_MODES, true)) {
            if (!is_string($expected)) {
                throw new InvalidArgumentException(sprintf(
                    'Match mode %s requires a string expected value',
                    $mode->name,
                ));
            }

            if ($mode === MatchMode::REGEXP && @preg_match($expected, '') === false) {
                throw new InvalidArgumentException(sprintf('Invalid regular expression: %s', $expected));
            }
        }

        $this->segments = $segments;
    }

    public function matches(Attributes $attributes): bool
    {
        return $this->evaluate(AttributeMatch::resolve($attributes, $this->segments));
    }

    /**
     * @param mixed $value the resolved OpenTelemetry AnyValue (or {@see AttributeMatch::MISSING})
     */
    private function evaluate(mixed $value): bool
    {
        return match ($this->mode) {
            MatchMode::EQUAL => AttributeMatch::equal($value, $this->expected),
            MatchMode::NOT_EQUAL => AttributeMatch::notEqual($value, $this->expected),
            MatchMode::GREATER_THAN => AttributeMatch::greaterThan($value, $this->expected),
            MatchMode::GREATER_THAN_EQUAL => AttributeMatch::greaterThanEqual($value, $this->expected),
            MatchMode::LESS_THAN => AttributeMatch::lessThan($value, $this->expected),
            MatchMode::LESS_THAN_EQUAL => AttributeMatch::lessThanEqual($value, $this->expected),
            MatchMode::REGEXP => AttributeMatch::regexp(AttributeMatch::stringForm($value), $this->patternExpected()),
            MatchMode::STARTS_WITH => AttributeMatch::startsWith(
                AttributeMatch::stringForm($value),
                $this->patternExpected(),
                $this->caseSensitive,
            ),
            MatchMode::ENDS_WITH => AttributeMatch::endsWith(
                AttributeMatch::stringForm($value),
                $this->patternExpected(),
                $this->caseSensitive,
            ),
            MatchMode::CONTAINS => AttributeMatch::contains(
                AttributeMatch::stringForm($value),
                $this->patternExpected(),
                $this->caseSensitive,
            ),
        };
    }

    /**
     * Render this rule as an inlined PHP boolean expression that reproduces
     * {@see AttributeMatch} semantics without the per-call dispatch of the
     * interpreted path. Expected values are scalar and emitted via
     * {@see var_export()}, never concatenated raw.
     *
     * The generated code is kept in lock-step with the interpreted path by the
     * compilation parity test; both must agree for every mode and value shape.
     *
     * @throws NotCompilable when the expected value is a DateTime, which cannot
     *                       be safely inlined as a literal
     */
    public function compile(Compilation $compilation): string
    {
        if ($this->expected instanceof DateTimeInterface) {
            throw new NotCompilable('DateTime expected values cannot be inlined');
        }

        $value = $this->resolveExpression($compilation);
        $expected = var_export($this->expected, true);

        return match ($this->mode) {
            // MISSING and present values are both never === a real expected, so the
            // MISSING guard is redundant here.
            MatchMode::EQUAL => sprintf('(%s === %s)', $value, $expected),
            MatchMode::NOT_EQUAL => $this->compileNotEqual($compilation, $value, $expected),
            MatchMode::GREATER_THAN => $this->compileOrder($compilation, $value, '>', $expected),
            MatchMode::GREATER_THAN_EQUAL => $this->compileOrder($compilation, $value, '>=', $expected),
            MatchMode::LESS_THAN => $this->compileOrder($compilation, $value, '<', $expected),
            MatchMode::LESS_THAN_EQUAL => $this->compileOrder($compilation, $value, '<=', $expected),
            MatchMode::REGEXP => $this->compileRegexp($compilation, $value),
            MatchMode::STARTS_WITH => $this->compileSubstr($compilation, $value, 'str_starts_with'),
            MatchMode::ENDS_WITH => $this->compileSubstr($compilation, $value, 'str_ends_with'),
            MatchMode::CONTAINS => $this->compileSubstr($compilation, $value, 'str_contains'),
        };
    }

    /**
     * Expression yielding the value at the path, or {@see AttributeMatch::MISSING}
     * when absent - the same contract as {@see AttributeMatch::resolve()}.
     *
     * A single-segment top-level lookup is inlined to get() (top-level attributes
     * are never null, so a null result means absent). Nested paths delegate to
     * resolve(), which preserves the absent-vs-present-null distinction.
     */
    private function resolveExpression(Compilation $compilation): string
    {
        if (count($this->segments) === 1) {
            return sprintf(
                '(%s[%s] ?? \%s::MISSING)',
                $compilation->values,
                var_export($this->segments[0], true),
                AttributeMatch::class,
            );
        }

        return sprintf(
            '\%s::resolve(%s, %s)',
            AttributeMatch::class,
            $compilation->subject,
            var_export($this->segments, true),
        );
    }

    private function compileNotEqual(Compilation $compilation, string $value, string $expected): string
    {
        $v = $compilation->temp();

        return sprintf('((%s = %s) !== \%s::MISSING && %s !== %s)', $v, $value, AttributeMatch::class, $v, $expected);
    }

    private function compileOrder(Compilation $compilation, string $value, string $operator, string $expected): string
    {
        $v = $compilation->temp();

        // is_scalar($v) === is_int || is_float || is_string || is_bool - the orderable
        // scalar set from AttributeMatch::order(), in one call instead of four.
        return sprintf(
            '((%s = %s) !== \%s::MISSING && (is_scalar(%s) || %s instanceof \DateTimeInterface) && %s %s %s)',
            $v,
            $value,
            AttributeMatch::class,
            $v,
            $v,
            $v,
            $operator,
            $expected,
        );
    }

    private function compileSubstr(Compilation $compilation, string $value, string $function): string
    {
        $v = $compilation->temp();
        /** @var string $needle pattern modes are validated to a string in the constructor */
        $needle = $this->expected;

        if ($this->caseSensitive) {
            return sprintf(
                '((%s = \%s::stringForm(%s)) !== null && %s(%s, %s))',
                $v,
                AttributeMatch::class,
                $value,
                $function,
                $v,
                var_export($needle, true),
            );
        }

        return sprintf(
            '((%s = \%s::stringForm(%s)) !== null && %s(mb_strtolower(%s), %s))',
            $v,
            AttributeMatch::class,
            $value,
            $function,
            $v,
            var_export(mb_strtolower($needle), true),
        );
    }

    private function compileRegexp(Compilation $compilation, string $value): string
    {
        $v = $compilation->temp();

        return sprintf(
            '((%s = \%s::stringForm(%s)) !== null && preg_match(%s, %s) === 1)',
            $v,
            AttributeMatch::class,
            $value,
            var_export($this->expected, true),
            $v,
        );
    }

    private function patternExpected(): string
    {
        return is_string($this->expected) ? $this->expected : '';
    }
}
