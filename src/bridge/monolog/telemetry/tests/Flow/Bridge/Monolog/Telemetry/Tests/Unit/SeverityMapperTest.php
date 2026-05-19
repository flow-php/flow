<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use Flow\Bridge\Monolog\Telemetry\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Telemetry\SeverityMapper;
use Flow\Telemetry\Logger\Severity;
use Generator;
use Monolog\Level;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(SeverityMapper::class)]
final class SeverityMapperTest extends TestCase
{
    /**
     * @return \Generator<string, array{Level, Severity}>
     */
    public static function defaultMappingProvider(): Generator
    {
        yield 'DEBUG → DEBUG' => [Level::Debug, Severity::DEBUG];
        yield 'INFO → INFO' => [Level::Info, Severity::INFO];
        yield 'NOTICE → INFO' => [Level::Notice, Severity::INFO];
        yield 'WARNING → WARN' => [Level::Warning, Severity::WARN];
        yield 'ERROR → ERROR' => [Level::Error, Severity::ERROR];
        yield 'CRITICAL → FATAL' => [Level::Critical, Severity::FATAL];
        yield 'ALERT → FATAL' => [Level::Alert, Severity::FATAL];
        yield 'EMERGENCY → FATAL' => [Level::Emergency, Severity::FATAL];
    }

    public function test_custom_mapping_overrides_default(): void
    {
        $customMapping = [
            Level::Debug->value => Severity::TRACE,
            Level::Info->value => Severity::INFO,
            Level::Notice->value => Severity::WARN,
            Level::Warning->value => Severity::WARN,
            Level::Error->value => Severity::ERROR,
            Level::Critical->value => Severity::FATAL,
            Level::Alert->value => Severity::FATAL,
            Level::Emergency->value => Severity::FATAL,
        ];

        $mapper = new SeverityMapper($customMapping);

        static::assertSame(Severity::TRACE, $mapper->map(Level::Debug));
        static::assertSame(Severity::WARN, $mapper->map(Level::Notice));
    }

    #[DataProvider('defaultMappingProvider')]
    public function test_default_mapping(Level $monologLevel, Severity $expectedSeverity): void
    {
        $mapper = new SeverityMapper();

        static::assertSame($expectedSeverity, $mapper->map($monologLevel));
    }

    public function test_default_mapping_returns_all_monolog_levels(): void
    {
        $mapping = SeverityMapper::defaultMapping();

        static::assertArrayHasKey(Level::Debug->value, $mapping);
        static::assertArrayHasKey(Level::Info->value, $mapping);
        static::assertArrayHasKey(Level::Notice->value, $mapping);
        static::assertArrayHasKey(Level::Warning->value, $mapping);
        static::assertArrayHasKey(Level::Error->value, $mapping);
        static::assertArrayHasKey(Level::Critical->value, $mapping);
        static::assertArrayHasKey(Level::Alert->value, $mapping);
        static::assertArrayHasKey(Level::Emergency->value, $mapping);
    }

    public function test_partial_custom_mapping_throws_on_missing_level(): void
    {
        $customMapping = [
            Level::Debug->value => Severity::DEBUG,
        ];

        $mapper = new SeverityMapper($customMapping);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No mapping defined for Monolog level: Info');

        $mapper->map(Level::Info);
    }
}
