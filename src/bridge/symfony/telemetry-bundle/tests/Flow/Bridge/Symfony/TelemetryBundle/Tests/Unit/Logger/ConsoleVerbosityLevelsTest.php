<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Logger;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleVerbosityLevels;
use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Output\OutputInterface;

#[CoversClass(ConsoleVerbosityLevels::class)]
final class ConsoleVerbosityLevelsTest extends TestCase
{
    public function test_default_thresholds(): void
    {
        $levels = ConsoleVerbosityLevels::default();

        static::assertSame(Severity::ERROR, $levels->thresholdFor(OutputInterface::VERBOSITY_QUIET));
        static::assertSame(Severity::ERROR, $levels->thresholdFor(OutputInterface::VERBOSITY_NORMAL));
        static::assertSame(Severity::WARN, $levels->thresholdFor(OutputInterface::VERBOSITY_VERBOSE));
        static::assertSame(Severity::INFO, $levels->thresholdFor(OutputInterface::VERBOSITY_VERY_VERBOSE));
        static::assertSame(Severity::DEBUG, $levels->thresholdFor(OutputInterface::VERBOSITY_DEBUG));
    }

    public function test_from_overrides_merges_onto_defaults(): void
    {
        $levels = ConsoleVerbosityLevels::fromOverrides([
            'VERBOSITY_NORMAL' => 'INFO',
            'VERBOSITY_VERBOSE' => 'DEBUG',
        ]);

        static::assertSame(Severity::INFO, $levels->thresholdFor(OutputInterface::VERBOSITY_NORMAL));
        static::assertSame(Severity::DEBUG, $levels->thresholdFor(OutputInterface::VERBOSITY_VERBOSE));
        static::assertSame(Severity::ERROR, $levels->thresholdFor(OutputInterface::VERBOSITY_QUIET));
    }

    public function test_from_overrides_rejects_unknown_severity_name(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown severity "NOTICE"');

        ConsoleVerbosityLevels::fromOverrides(['VERBOSITY_NORMAL' => 'NOTICE']);
    }

    public function test_from_overrides_rejects_unknown_verbosity_level(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown console verbosity level "VERBOSITY_LOUD"');

        ConsoleVerbosityLevels::fromOverrides(['VERBOSITY_LOUD' => 'INFO']);
    }

    public function test_unknown_verbosity_falls_back_to_warn(): void
    {
        static::assertSame(Severity::WARN, ConsoleVerbosityLevels::default()->thresholdFor(999));
    }
}
