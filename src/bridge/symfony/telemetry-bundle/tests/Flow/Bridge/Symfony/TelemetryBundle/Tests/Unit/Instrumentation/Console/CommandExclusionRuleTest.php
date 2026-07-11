<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\CommandExclusionRule;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(CommandExclusionRule::class)]
final class CommandExclusionRuleTest extends TestCase
{
    #[TestWith(['messenger:consume', 'messenger:consume', true])]
    #[TestWith(['messenger:consume', 'messenger:consume-failed', false])]
    #[TestWith(['messenger:consume', 'cache:clear', false])]
    #[TestWith(['/^debug:/', 'debug:router', true])]
    #[TestWith(['/^debug:/', 'app:debug', false])]
    #[TestWith(['~^cache:~', 'cache:warmup', true])]
    #[TestWith(['/^messenger:consume$/', 'messenger:consume', true])]
    #[TestWith(['/^messenger:consume$/', 'messenger:consume-failed', false])]
    public function test_matches(string $pattern, string $commandName, bool $expected): void
    {
        static::assertSame($expected, (new CommandExclusionRule($pattern))->matches($commandName));
    }
}
