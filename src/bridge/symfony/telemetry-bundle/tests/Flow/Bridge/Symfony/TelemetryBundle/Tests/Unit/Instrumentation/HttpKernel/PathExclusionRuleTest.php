<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\PathExclusionRule;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(PathExclusionRule::class)]
final class PathExclusionRuleTest extends TestCase
{
    public function test_exact_path_match_excludes_all_methods() : void
    {
        $rule = new PathExclusionRule('/_wdt');

        self::assertTrue($rule->matches('/_wdt', 'GET'));
        self::assertTrue($rule->matches('/_wdt', 'POST'));
        self::assertTrue($rule->matches('/_wdt', 'DELETE'));
        self::assertFalse($rule->matches('/_profiler', 'GET'));
        self::assertFalse($rule->matches('/_wdt/abc', 'GET'));
    }

    public function test_exact_path_match_with_method_filter() : void
    {
        $rule = new PathExclusionRule('/_wdt', 'GET');

        self::assertTrue($rule->matches('/_wdt', 'GET'));
        self::assertFalse($rule->matches('/_wdt', 'POST'));
        self::assertFalse($rule->matches('/_wdt', 'DELETE'));
    }

    public function test_from_config_with_null_method() : void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt', 'method' => null]);

        self::assertSame('/_wdt', $rule->path);
        self::assertNull($rule->method);
    }

    public function test_from_config_with_path_and_method() : void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt', 'method' => 'GET']);

        self::assertSame('/_wdt', $rule->path);
        self::assertSame('GET', $rule->method);
    }

    public function test_from_config_with_path_only() : void
    {
        $rule = PathExclusionRule::fromConfig(['path' => '/_wdt']);

        self::assertSame('/_wdt', $rule->path);
        self::assertNull($rule->method);
    }

    public function test_method_comparison_is_case_insensitive() : void
    {
        $rule = new PathExclusionRule('/_wdt', 'GET');

        self::assertTrue($rule->matches('/_wdt', 'GET'));
        self::assertTrue($rule->matches('/_wdt', 'get'));
        self::assertTrue($rule->matches('/_wdt', 'Get'));
    }

    public function test_regex_path_match_excludes_all_methods() : void
    {
        $rule = new PathExclusionRule('/^\/_profiler.*/');

        self::assertTrue($rule->matches('/_profiler', 'GET'));
        self::assertTrue($rule->matches('/_profiler/search', 'GET'));
        self::assertTrue($rule->matches('/_profiler/123/dump', 'POST'));
        self::assertFalse($rule->matches('/_wdt', 'GET'));
        self::assertFalse($rule->matches('/profiler', 'GET'));
    }

    public function test_regex_path_match_with_method_filter() : void
    {
        $rule = new PathExclusionRule('/^\/_profiler.*/', 'GET');

        self::assertTrue($rule->matches('/_profiler', 'GET'));
        self::assertTrue($rule->matches('/_profiler/search', 'GET'));
        self::assertFalse($rule->matches('/_profiler/123/dump', 'POST'));
        self::assertFalse($rule->matches('/_profiler', 'DELETE'));
    }
}
