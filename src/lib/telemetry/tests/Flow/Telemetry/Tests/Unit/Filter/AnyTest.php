<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\Compilation;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Filter\NotCompilable;
use Flow\Telemetry\Tests\Mother\FixedMatcher;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_rule;

final class AnyTest extends TestCase
{
    public function test_compile_throws_when_a_child_is_not_compilable(): void
    {
        $this->expectException(NotCompilable::class);

        any(attribute_rule('a', MatchMode::EQUAL, 1), new FixedMatcher(true))->compile(new Compilation('$a', '$__v'));
    }

    public function test_compile_joins_children_with_logical_or(): void
    {
        static::assertSame(
            '((($__v[\'a\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === 1)) || ((($__v[\'b\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === 2))',
            any(
                attribute_rule('a', MatchMode::EQUAL, 1),
                attribute_rule('b', MatchMode::EQUAL, 2),
            )->compile(new Compilation('$a', '$__v')),
        );
    }

    public function test_does_not_match_when_no_child_matches(): void
    {
        static::assertFalse(any(new FixedMatcher(false), new FixedMatcher(false))->matches(Attributes::create([])));
    }

    public function test_matches_when_one_child_matches(): void
    {
        static::assertTrue(any(new FixedMatcher(false), new FixedMatcher(true))->matches(Attributes::create([])));
    }

    public function test_throws_on_no_matchers(): void
    {
        $this->expectException(InvalidArgumentException::class);

        any();
    }
}
