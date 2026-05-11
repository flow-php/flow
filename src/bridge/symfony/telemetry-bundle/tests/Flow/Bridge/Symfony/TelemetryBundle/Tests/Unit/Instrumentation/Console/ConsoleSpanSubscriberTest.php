<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleSpanSubscriber;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
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
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:process');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_exit_code_nonzero_sets_error_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

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
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear']);

        $command = new Command('cache:clear');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_matches_pattern_regex_match(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['/^cache:.*/']);

        $command = new Command('cache:warmup');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_on_error_does_nothing_when_no_active_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['cache:clear']);

        $command = new Command('cache:clear');
        $exception = new \RuntimeException('Error');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $errorEvent = new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), $exception, $command);

        $subscriber->onCommand($commandEvent);
        $subscriber->onError($errorEvent);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_on_error_records_exception(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:failing');
        $exception = new \RuntimeException('Something went wrong');
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

    public function test_on_terminate_does_nothing_when_no_active_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

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
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['assets:install']);

        $command = new Command('assets:install');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_excludes_regex_matches(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry, excludeCommands: ['/^debug:/']);

        $command = new Command('debug:router');
        $event = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());

        $subscriber->onCommand($event);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_includes_non_excluded(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

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
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:task');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(Command::class, $spans[0]->attributes()['command.class']);
    }

    public function test_span_includes_command_name_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:sync');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('app:sync', $spans[0]->attributes()['command.name']);
    }

    public function test_span_includes_exit_code_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $subscriber = new ConsoleSpanSubscriber($telemetry);

        $command = new Command('app:task');
        $commandEvent = new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput());
        $terminateEvent = new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 42);

        $subscriber->onCommand($commandEvent);
        $subscriber->onTerminate($terminateEvent);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(42, $spans[0]->attributes()['process.exit_code']);
    }

    public function test_span_kind_is_internal(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

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
        $telemetry = $this->createTelemetry($spanProcessor);

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

    private function createTelemetry(MemorySpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
