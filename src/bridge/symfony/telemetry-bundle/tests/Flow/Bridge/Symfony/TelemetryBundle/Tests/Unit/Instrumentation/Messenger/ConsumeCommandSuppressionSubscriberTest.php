<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\ConsumeCommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\ConsumeMessagesCommandMother;
use Flow\Telemetry\Context\MemoryContextStorage;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\Messenger\Command\ConsumeMessagesCommand;

use function class_exists;

#[CoversClass(ConsumeCommandSuppressionSubscriber::class)]
final class ConsumeCommandSuppressionSubscriberTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(ConsumeMessagesCommand::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }
    }

    public function test_suppresses_tracing_on_consume_command(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new ConsumeCommandSuppressionSubscriber($storage);

        static::assertFalse($storage->current()->isTracingSuppressed());

        $subscriber->onCommand(
            new ConsoleCommandEvent(ConsumeMessagesCommandMother::create(), new ArrayInput([]), new NullOutput()),
        );

        static::assertTrue($storage->current()->isTracingSuppressed());
    }

    public function test_ignores_non_consume_commands(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new ConsumeCommandSuppressionSubscriber($storage);

        $subscriber->onCommand(
            new ConsoleCommandEvent(new Command('app:something'), new ArrayInput([]), new NullOutput()),
        );

        static::assertFalse($storage->current()->isTracingSuppressed());
    }

    public function test_detaches_suppression_on_terminate(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new ConsumeCommandSuppressionSubscriber($storage);
        $command = ConsumeMessagesCommandMother::create();

        $subscriber->onCommand(new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput()));

        static::assertTrue($storage->current()->isTracingSuppressed());

        $subscriber->onTerminate(new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0));

        static::assertFalse($storage->current()->isTracingSuppressed());
    }
}
