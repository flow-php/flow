<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleLogOutputSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleOutputLogProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleVerbosityLevels;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

#[CoversClass(ConsoleLogOutputSubscriber::class)]
final class ConsoleLogOutputSubscriberTest extends TestCase
{
    public function test_on_command_pushes_the_output_into_the_processor(): void
    {
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $output = new BufferedOutput();

        (new ConsoleLogOutputSubscriber($processor))->onCommand(
            new ConsoleCommandEvent(new Command('t'), new ArrayInput([]), $output),
        );

        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));

        static::assertStringContainsString('error-message', $output->fetch());
    }

    public function test_on_terminate_clears_the_output_from_the_processor(): void
    {
        $processor = new ConsoleOutputLogProcessor(ConsoleVerbosityLevels::default());
        $output = new BufferedOutput();
        $subscriber = new ConsoleLogOutputSubscriber($processor);

        $subscriber->onCommand(new ConsoleCommandEvent(new Command('t'), new ArrayInput([]), $output));
        $subscriber->onTerminate(new ConsoleTerminateEvent(new Command('t'), new ArrayInput([]), $output, 0));

        $processor->process(LogEntryMother::with(Severity::ERROR, 'error-message'));

        static::assertSame('', $output->fetch());
    }

    public function test_subscribes_to_command_and_terminate(): void
    {
        $events = ConsoleLogOutputSubscriber::getSubscribedEvents();

        static::assertArrayHasKey(ConsoleEvents::COMMAND, $events);
        static::assertArrayHasKey(ConsoleEvents::TERMINATE, $events);
    }
}
