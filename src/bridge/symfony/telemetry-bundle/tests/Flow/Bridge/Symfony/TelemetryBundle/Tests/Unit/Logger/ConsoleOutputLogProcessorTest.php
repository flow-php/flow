<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Logger;

use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleOutputLogProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleVerbosityLevels;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\OutputInterface;

#[CoversClass(ConsoleOutputLogProcessor::class)]
final class ConsoleOutputLogProcessorTest extends TestCase
{
    public function test_debug_verbosity_writes_debug(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_DEBUG);
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::DEBUG, 'debug-message'));

        static::assertStringContainsString('debug-message', $output->fetch());
    }

    public function test_flush_is_a_noop_returning_true(): void
    {
        static::assertTrue((new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default()))->flush());
    }

    public function test_includes_attributes(): void
    {
        $output = new BufferedOutput();
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::ERROR, 'with-attrs', ['user.id' => 42]));

        static::assertStringContainsString('user.id=42', $output->fetch());
    }

    public function test_normal_verbosity_skips_warning(): void
    {
        $output = new BufferedOutput();
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::WARN, 'warn-message'));

        static::assertStringNotContainsString('warn-message', $output->fetch());
    }

    public function test_normal_verbosity_writes_error(): void
    {
        $output = new BufferedOutput();
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));

        $written = $output->fetch();
        static::assertStringContainsString('ERROR', $written);
        static::assertStringContainsString('error-message', $written);
    }

    public function test_nothing_written_after_output_is_cleared(): void
    {
        $output = new BufferedOutput();
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);
        $processor->clearOutput();

        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));

        static::assertSame('', $output->fetch());
    }

    public function test_nothing_written_when_no_output_is_set(): void
    {
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());

        $this->expectNotToPerformAssertions();

        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));
    }

    public function test_quiet_verbosity_shows_only_errors(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_QUIET);
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::WARN, 'warn-message'));
        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));

        $written = $output->fetch();
        static::assertStringNotContainsString('warn-message', $written);
        static::assertStringContainsString('error-message', $written);
    }

    public function test_user_angle_brackets_in_body_are_escaped(): void
    {
        $output = new BufferedOutput();
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::ERROR, 'rendered <script> tag'));

        static::assertStringContainsString('<script>', $output->fetch());
    }

    public function test_verbose_verbosity_writes_warning_but_not_info(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_VERBOSE);
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::WARN, 'warn-message'));
        $processor->process(LogEntryMother::with(Severity::INFO, 'info-message'));

        $written = $output->fetch();
        static::assertStringContainsString('warn-message', $written);
        static::assertStringNotContainsString('info-message', $written);
    }

    public function test_very_verbose_verbosity_writes_info_but_not_debug(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_VERY_VERBOSE);
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $processor->setOutput($output);

        $processor->process(LogEntryMother::with(Severity::INFO, 'info-message'));
        $processor->process(LogEntryMother::with(Severity::DEBUG, 'debug-message'));

        $written = $output->fetch();
        static::assertStringContainsString('info-message', $written);
        static::assertStringNotContainsString('debug-message', $written);
    }
}
