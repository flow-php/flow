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

use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\attribute_rule;

final class AllTest extends TestCase
{
    public function test_compile_throws_when_a_child_is_not_compilable(): void
    {
        $this->expectException(NotCompilable::class);

        all(attribute_rule('a', MatchMode::EQUAL, 1), new FixedMatcher(true))->compile(new Compilation('$a', '$__v'));
    }

    public function test_compile_joins_children_with_logical_and(): void
    {
        static::assertSame(
            '((($__v[\'a\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === 1)) && ((($__v[\'b\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === 2))',
            all(
                attribute_rule('a', MatchMode::EQUAL, 1),
                attribute_rule('b', MatchMode::EQUAL, 2),
            )->compile(new Compilation('$a', '$__v')),
        );
    }

    public function test_does_not_match_when_one_child_fails(): void
    {
        static::assertFalse(all(new FixedMatcher(true), new FixedMatcher(false))->matches(Attributes::create([])));
    }

    public function test_matches_when_every_child_matches(): void
    {
        static::assertTrue(all(new FixedMatcher(true), new FixedMatcher(true))->matches(Attributes::create([])));
    }

    public function test_throws_on_no_matchers(): void
    {
        $this->expectException(InvalidArgumentException::class);

        all();
    }
}
