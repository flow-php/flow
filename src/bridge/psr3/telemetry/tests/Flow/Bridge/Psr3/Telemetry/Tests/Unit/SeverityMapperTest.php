<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Unit;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Bridge\Psr3\Telemetry\SeverityMapper;
use Flow\Telemetry\Logger\Severity;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use Stringable;

final class SeverityMapperTest extends TestCase
{
    /**
     * @return \Generator<string, array{string, Severity}>
     */
    public static function default_mapping_provider(): Generator
    {
        yield 'debug' => [LogLevel::DEBUG, Severity::DEBUG];
        yield 'info' => [LogLevel::INFO, Severity::INFO];
        yield 'notice' => [LogLevel::NOTICE, Severity::INFO];
        yield 'warning' => [LogLevel::WARNING, Severity::WARN];
        yield 'error' => [LogLevel::ERROR, Severity::ERROR];
        yield 'critical' => [LogLevel::CRITICAL, Severity::FATAL];
        yield 'alert' => [LogLevel::ALERT, Severity::FATAL];
        yield 'emergency' => [LogLevel::EMERGENCY, Severity::FATAL];
    }

    public function test_accepts_stringable_level(): void
    {
        $mapper = new SeverityMapper();
        $level = new class implements Stringable {
            public function __toString(): string
            {
                return LogLevel::WARNING;
            }
        };

        static::assertSame(Severity::WARN, $mapper->map($level));
    }

    public function test_custom_mapping_overrides_defaults(): void
    {
        $mapper = new SeverityMapper([
            LogLevel::DEBUG => Severity::TRACE,
            LogLevel::INFO => Severity::INFO,
            LogLevel::NOTICE => Severity::WARN,
            LogLevel::WARNING => Severity::WARN,
            LogLevel::ERROR => Severity::ERROR,
            LogLevel::CRITICAL => Severity::FATAL,
            LogLevel::ALERT => Severity::FATAL,
            LogLevel::EMERGENCY => Severity::FATAL,
        ]);

        static::assertSame(Severity::TRACE, $mapper->map(LogLevel::DEBUG));
        static::assertSame(Severity::WARN, $mapper->map(LogLevel::NOTICE));
    }

    public function test_default_mapping_exposes_full_psr3_level_set(): void
    {
        $mapping = SeverityMapper::defaultMapping();

        static::assertSame(
            [
                LogLevel::DEBUG,
                LogLevel::INFO,
                LogLevel::NOTICE,
                LogLevel::WARNING,
                LogLevel::ERROR,
                LogLevel::CRITICAL,
                LogLevel::ALERT,
                LogLevel::EMERGENCY,
            ],
            array_keys($mapping),
        );
    }

    #[DataProvider('default_mapping_provider')]
    public function test_default_mapping_matches_psr3_to_otel_severity(string $level, Severity $expected): void
    {
        $mapper = new SeverityMapper();

        static::assertSame($expected, $mapper->map($level));
    }

    public function test_throws_on_unknown_level(): void
    {
        $mapper = new SeverityMapper();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown PSR-3 log level: verbose');

        $mapper->map('verbose');
    }

    public function test_throws_when_custom_mapping_lacks_level(): void
    {
        $mapper = new SeverityMapper([
            LogLevel::ERROR => Severity::ERROR,
        ]);

        $this->expectException(InvalidArgumentException::class);

        $mapper->map(LogLevel::DEBUG);
    }
}
