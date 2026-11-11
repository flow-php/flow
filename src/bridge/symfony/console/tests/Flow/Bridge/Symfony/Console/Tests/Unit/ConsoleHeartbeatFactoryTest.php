<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\ConsoleHeartbeatFactory;
use Flow\Bridge\Symfony\Console\ConsoleStyle;
use Flow\Bridge\Symfony\Console\LogHeartbeat;
use Flow\Bridge\Symfony\Console\StatusLineHeartbeat;
use Flow\Bridge\Symfony\Console\TaskListHeartbeat;
use Flow\Bridge\Symfony\Console\Tests\Context\TerminalContext;
use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\StreamOutput;

use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function Flow\Types\DSL\type_instance_of;
use function str_contains;

final class ConsoleHeartbeatFactoryTest extends FlowTestCase
{
    /**
     * @param class-string $class
     */
    #[TestWith([ConsoleStyle::tasks, TaskListHeartbeat::class])]
    #[TestWith([ConsoleStyle::graph, TaskListHeartbeat::class])]
    #[TestWith([ConsoleStyle::line, StatusLineHeartbeat::class])]
    #[TestWith([ConsoleStyle::log, LogHeartbeat::class])]
    public function test_each_style_on_a_terminal(ConsoleStyle $style, string $class): void
    {
        $terminal = new TerminalContext(100);

        try {
            static::assertInstanceOf($class, console_heartbeat($terminal->stream(), $style));
        } finally {
            $terminal->restore();
        }
    }

    #[TestWith([ConsoleStyle::tasks])]
    #[TestWith([ConsoleStyle::graph])]
    #[TestWith([ConsoleStyle::line])]
    public function test_a_non_terminal_gets_the_log(ConsoleStyle $style): void
    {
        static::assertInstanceOf(LogHeartbeat::class, (new ConsoleHeartbeatFactory())->create(
            new BufferedOutput(),
            $style,
        ));
    }

    #[TestWith([ConsoleStyle::tasks])]
    #[TestWith([ConsoleStyle::graph])]
    public function test_a_section_of_the_caller_gets_the_line(ConsoleStyle $style): void
    {
        $terminal = new TerminalContext(100);

        try {
            static::assertInstanceOf(StatusLineHeartbeat::class, (new ConsoleHeartbeatFactory())->create(
                $terminal->section(),
                $style,
            ));
        } finally {
            $terminal->restore();
        }
    }

    public function test_the_default_style_is_tasks(): void
    {
        $terminal = new TerminalContext(100);

        try {
            static::assertInstanceOf(TaskListHeartbeat::class, console_heartbeat($terminal->stream()));
        } finally {
            $terminal->restore();
        }
    }

    #[TestWith([ConsoleStyle::tasks])]
    #[TestWith([ConsoleStyle::graph])]
    #[TestWith([ConsoleStyle::line])]
    #[TestWith([ConsoleStyle::log])]
    public function test_a_console_output_draws_on_its_error_output(ConsoleStyle $style): void
    {
        $terminal = new TerminalContext(100);

        try {
            $console = $terminal->console();

            console_heartbeat($console, $style)->beat(PulseMother::pulse(elapsedSeconds: 3.0, stages: [
                StageProgressMother::done(StageName::read, 3, endedAtSeconds: 1.0),
            ]));

            static::assertSame(['', true], [
                $terminal->raw($console),
                str_contains(
                    $terminal->text(type_instance_of(StreamOutput::class)->assert($console->getErrorOutput())),
                    'read',
                ),
            ]);
        } finally {
            $terminal->restore();
        }
    }
}
