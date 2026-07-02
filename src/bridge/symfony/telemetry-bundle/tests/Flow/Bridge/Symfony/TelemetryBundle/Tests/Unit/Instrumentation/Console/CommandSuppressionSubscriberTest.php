<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\CommandSuppressionSubscriber;
use Flow\Telemetry\Context\MemoryContextStorage;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

#[CoversClass(CommandSuppressionSubscriber::class)]
final class CommandSuppressionSubscriberTest extends TestCase
{
    public function test_suppresses_tracing_for_a_configured_command(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new CommandSuppressionSubscriber($storage, ['messenger:consume']);

        static::assertFalse($storage->current()->isTracingSuppressed());

        $subscriber->onCommand(
            new ConsoleCommandEvent(new Command('messenger:consume'), new ArrayInput([]), new NullOutput()),
        );

        static::assertTrue($storage->current()->isTracingSuppressed());
    }

    public function test_matches_a_regex_pattern(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new CommandSuppressionSubscriber($storage, ['/^app:worker/']);

        $subscriber->onCommand(
            new ConsoleCommandEvent(new Command('app:worker:emails'), new ArrayInput([]), new NullOutput()),
        );

        static::assertTrue($storage->current()->isTracingSuppressed());
    }

    public function test_ignores_commands_not_in_the_list(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new CommandSuppressionSubscriber($storage, ['messenger:consume']);

        $subscriber->onCommand(
            new ConsoleCommandEvent(new Command('cache:clear'), new ArrayInput([]), new NullOutput()),
        );

        static::assertFalse($storage->current()->isTracingSuppressed());
    }

    public function test_detaches_suppression_on_terminate(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new CommandSuppressionSubscriber($storage, ['messenger:consume']);
        $command = new Command('messenger:consume');

        $subscriber->onCommand(new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput()));

        static::assertTrue($storage->current()->isTracingSuppressed());

        $subscriber->onTerminate(new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0));

        static::assertFalse($storage->current()->isTracingSuppressed());
    }
}
