<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\PathExclusionRule;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(PathExclusionRule::class)]
final class PathExclusionRuleTest extends TestCase
{
    public function test_exact_path_match_excludes_all_methods(): void
    {
        $rule = new PathExclusionRule('/_wdt');

        static::assertTrue($rule->matches('/_wdt', 'GET'));
        static::assertTrue($rule->matches('/_wdt', 'POST'));
        static::assertTrue($rule->matches('/_wdt', 'DELETE'));
        static::assertFalse($rule->matches('/_profiler', 'GET'));
        static::assertFalse($rule->matches('/_wdt/abc', 'GET'));
    }

    public function test_exact_path_match_with_method_filter(): void
    {
        $rule = new PathExclusionRule('/_wdt', 'GET');

        static::assertTrue($rule->matches('/_wdt', 'GET'));
        static::assertFalse($rule->matches('/_wdt', 'POST'));
        static::assertFalse($rule->matches('/_wdt', 'DELETE'));
    }

    public function test_from_config_with_null_method(): void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt', 'method' => null]);

        static::assertSame('/_wdt', $rule->path);
        static::assertNull($rule->method);
    }

    public function test_from_config_with_path_and_method(): void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt', 'method' => 'GET']);

        static::assertSame('/_wdt', $rule->path);
        static::assertSame('GET', $rule->method);
    }

    public function test_from_config_with_path_only(): void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt']);

        static::assertSame('/_wdt', $rule->path);
        static::assertNull($rule->method);
    }

    public function test_method_comparison_is_case_insensitive(): void
    {
        $rule = new PathExclusionRule('/_wdt', 'GET');

        static::assertTrue($rule->matches('/_wdt', 'GET'));
        static::assertTrue($rule->matches('/_wdt', 'get'));
        static::assertTrue($rule->matches('/_wdt', 'Get'));
    }

    public function test_regex_path_match_excludes_all_methods(): void
    {
        $rule = new PathExclusionRule('#^\/_profiler.*#');

        static::assertTrue($rule->matches('/_profiler', 'GET'));
        static::assertTrue($rule->matches('/_profiler/search', 'GET'));
        static::assertTrue($rule->matches('/_profiler/123/dump', 'POST'));
        static::assertFalse($rule->matches('/_wdt', 'GET'));
        static::assertFalse($rule->matches('/profiler', 'GET'));
    }

    public function test_regex_path_match_with_method_filter(): void
    {
        $rule = new PathExclusionRule('/^\/_profiler.*/', 'GET');

        static::assertTrue($rule->matches('/_profiler', 'GET'));
        static::assertTrue($rule->matches('/_profiler/search', 'GET'));
        static::assertFalse($rule->matches('/_profiler/123/dump', 'POST'));
        static::assertFalse($rule->matches('/_profiler', 'DELETE'));
    }
}
