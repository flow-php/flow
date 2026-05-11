<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(Severity::class)]
final class SeverityTest extends TestCase
{
    /**
     * @return \Generator<string, array{Severity, Severity, bool}>
     */
    public static function isAtLeastProvider(): \Generator
    {
        yield 'TRACE is at least TRACE' => [Severity::TRACE, Severity::TRACE, true];
        yield 'TRACE is not at least DEBUG' => [Severity::TRACE, Severity::DEBUG, false];
        yield 'DEBUG is at least TRACE' => [Severity::DEBUG, Severity::TRACE, true];
        yield 'DEBUG is at least DEBUG' => [Severity::DEBUG, Severity::DEBUG, true];
        yield 'DEBUG is not at least INFO' => [Severity::DEBUG, Severity::INFO, false];
        yield 'INFO is at least TRACE' => [Severity::INFO, Severity::TRACE, true];
        yield 'INFO is at least DEBUG' => [Severity::INFO, Severity::DEBUG, true];
        yield 'INFO is at least INFO' => [Severity::INFO, Severity::INFO, true];
        yield 'INFO is not at least WARN' => [Severity::INFO, Severity::WARN, false];
        yield 'WARN is at least INFO' => [Severity::WARN, Severity::INFO, true];
        yield 'WARN is at least WARN' => [Severity::WARN, Severity::WARN, true];
        yield 'WARN is not at least ERROR' => [Severity::WARN, Severity::ERROR, false];
        yield 'ERROR is at least WARN' => [Severity::ERROR, Severity::WARN, true];
        yield 'ERROR is at least ERROR' => [Severity::ERROR, Severity::ERROR, true];
        yield 'ERROR is not at least FATAL' => [Severity::ERROR, Severity::FATAL, false];
        yield 'FATAL is at least ERROR' => [Severity::FATAL, Severity::ERROR, true];
        yield 'FATAL is at least FATAL' => [Severity::FATAL, Severity::FATAL, true];
    }

    /**
     * @return \Generator<string, array{Severity, string}>
     */
    public static function nameProvider(): \Generator
    {
        yield 'TRACE' => [Severity::TRACE, 'TRACE'];
        yield 'DEBUG' => [Severity::DEBUG, 'DEBUG'];
        yield 'INFO' => [Severity::INFO, 'INFO'];
        yield 'WARN' => [Severity::WARN, 'WARN'];
        yield 'ERROR' => [Severity::ERROR, 'ERROR'];
        yield 'FATAL' => [Severity::FATAL, 'FATAL'];
    }

    #[DataProvider('isAtLeastProvider')]
    public function test_is_at_least_compares_correctly(Severity $severity, Severity $other, bool $expected): void
    {
        static::assertSame($expected, $severity->isAtLeast($other));
    }

    #[DataProvider('nameProvider')]
    public function test_name_returns_correct_string(Severity $severity, string $expectedName): void
    {
        static::assertSame($expectedName, $severity->name());
    }
}
