<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\Compilation;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Filter\NotCompilable;
use Flow\Telemetry\Tests\Mother\FixedMatcher;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\not;

final class NotTest extends TestCase
{
    public function test_compile_throws_when_child_is_not_compilable(): void
    {
        $this->expectException(NotCompilable::class);

        not(new FixedMatcher(true))->compile(new Compilation('$a', '$__v'));
    }

    public function test_compile_negates_the_child_expression(): void
    {
        static::assertSame(
            '!((($__v[\'x\'] ?? \Flow\Telemetry\Filter\AttributeMatch::MISSING) === \'y\'))',
            not(attribute_rule('x', MatchMode::EQUAL, 'y'))->compile(new Compilation('$a', '$__v')),
        );
    }

    public function test_inverts_a_false_child(): void
    {
        static::assertTrue(not(new FixedMatcher(false))->matches(Attributes::create([])));
    }

    public function test_inverts_a_true_child(): void
    {
        static::assertFalse(not(new FixedMatcher(true))->matches(Attributes::create([])));
    }
}
