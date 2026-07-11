<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

#[CoversClass(ConsoleSpanSubscriber::class)]
final class ConsoleSpanSubscriberTest extends TestCase
{
    public function test_exit_code_0_sets_ok_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:process');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($spans[0]->status());
    }

    public function test_exit_code_nonzero_sets_error_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:process');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 1);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Exit code: 1', $status->description);
        static::assertSame('1', $spans[0]->attributes()['error.type']);
    }

    public function test_get_subscribed_events_returns_correct_events(): void
    {
        $events = ConsoleSpanSubscriber::getSubscribedEvents();

        static::assertArrayHasKey('console.command', $events);
        static::assertArrayHasKey('console.error', $events);
        static::assertArrayHasKey('console.terminate', $events);
        static::assertArrayHasKey('console.signal', $events);

        static::assertSame(['onCommand', 10000], $events['console.command']);
        static::assertSame(['onError', 0], $events['console.error']);
        static::assertSame(['onTerminate', -10000], $events['console.terminate']);
        static::assertSame(['onSignal', 0], $events['console.signal']);
    }

    public function test_matches_pattern_exact_match(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear']);

        $command = new Command('cache:clear');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_matches_pattern_regex_match(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['/^cache:.*/']);

        $command = new Command('cache:warmup');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_on_error_does_nothing_when_no_active_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear']);

        $command = new Command('cache:clear');
        $exception = new RuntimeException('Error');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $errorEvent = new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), $exception, $command);

        $subscriber->onCommand($commandEvent);
        $subscriber->onError($errorEvent);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_on_error_records_exception(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:failing');
        $exception = new RuntimeException('Something went wrong');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $errorEvent = new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), $exception, $command);
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 1);

        $subscriber->onCommand($commandEvent);
        $subscriber->onError($errorEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $events = $spans[0]->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());
    }

    public function test_nested_command_does_not_lose_the_outer_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $outer = new Command('app:outer');
        $nested = new Command('app:nested');

        // A command running another command via Application::run() dispatches balanced nested
        // COMMAND/TERMINATE pairs.
        $subscriber->onCommand(new ConsoleCommandEvent($outer, new ArrayInput([]), new NullOutput()));
        $subscriber->onCommand(new ConsoleCommandEvent($nested, new ArrayInput([]), new NullOutput()));
        $subscriber->onTerminate(new ConsoleTerminateEvent($nested, new ArrayInput([]), new NullOutput(), 0));
        $subscriber->onTerminate(new ConsoleTerminateEvent($outer, new ArrayInput([]), new NullOutput(), 0));

        $spans = $spanProcessor->endedSpans();

        static::assertCount(2, $spans);
        static::assertSame('app:nested', $spans[0]->name());
        static::assertSame('app:outer', $spans[1]->name());
        static::assertTrue(
            $spans[0]->context()->traceId->equals($spans[1]->context()->traceId),
            'the nested command span must share the outer command trace',
        );
        static::assertSame(
            $spans[1]->context()->spanId->toHex(),
            $spans[0]->context()->parentSpanId?->toHex(),
            'the nested command span must be a child of the outer command span',
        );
    }

    public function test_error_in_the_outer_command_after_a_nested_command_is_recorded(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $outer = new Command('app:outer');
        $nested = new Command('app:nested');
        $exception = new RuntimeException('outer failed');

        $subscriber->onCommand(new ConsoleCommandEvent($outer, new ArrayInput([]), new NullOutput()));
        $subscriber->onCommand(new ConsoleCommandEvent($nested, new ArrayInput([]), new NullOutput()));
        $subscriber->onTerminate(new ConsoleTerminateEvent($nested, new ArrayInput([]), new NullOutput(), 0));
        $subscriber->onError(new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), $exception, $outer));
        $subscriber->onTerminate(new ConsoleTerminateEvent($outer, new ArrayInput([]), new NullOutput(), 1));

        $spans = $spanProcessor->endedSpans();

        static::assertCount(2, $spans);

        $outerSpan = $spans[1];
        static::assertSame('app:outer', $outerSpan->name());
        static::assertCount(1, $outerSpan->events());
        static::assertSame('exception', $outerSpan->events()[0]->name());
    }

    public function test_error_type_uses_the_exception_class(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:failing');
        $exception = new RuntimeException('Something went wrong');

        $subscriber->onCommand(new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput()));
        $subscriber->onError(new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), $exception, $command));
        $subscriber->onTerminate(new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 1));

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        // When an exception caused the failure, error.type must be its class, not the exit code.
        static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
        static::assertSame('Something went wrong', $spans[0]->status()?->description);
    }

    public function test_on_terminate_does_nothing_when_no_active_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear']);

        $command = new Command('cache:clear');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_excludes_exact_matches(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['assets:install']);

        $command = new Command('assets:install');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_excludes_regex_matches(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['/^debug:/']);

        $command = new Command('debug:router');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_includes_non_excluded(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear', '/^debug:/']);

        $command = new Command('doctrine:migrations:migrate');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($event);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('doctrine:migrations:migrate', $spans[0]->name());
    }

    public function test_span_includes_command_class_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:task');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(Command::class, $spans[0]->attributes()['flow.symfony.command.class']);
    }

    public function test_span_includes_command_name_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:sync');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('app:sync', $spans[0]->attributes()['flow.symfony.command.name']);
    }

    public function test_span_includes_exit_code_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:task');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 42);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(42, $spans[0]->attributes()['process.exit.code']);
    }

    public function test_span_kind_is_internal(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:task');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
    }

    public function test_span_name_set_to_command_name(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:send-emails');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('app:send-emails', $spans[0]->name());
    }
}
