<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

final class ConsoleHeartbeatFactory
{
    public function create(OutputInterface $output, ConsoleStyle $style = ConsoleStyle::tasks): ConsoleHeartbeat
    {
        $target = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;

        if (!$target->isDecorated()) {
            return new LogHeartbeat($target);
        }

        // a caller's section counts its own lines; the task list's cursor moves would break that count
        $movesCursor = $target instanceof StreamOutput && !$target instanceof ConsoleSectionOutput;

        return match ($style) {
            ConsoleStyle::tasks => $movesCursor ? new TaskListHeartbeat($target) : new StatusLineHeartbeat($target),
            ConsoleStyle::graph => $movesCursor
                ? new TaskListHeartbeat($target, new ThroughputGraph())
                : new StatusLineHeartbeat($target),
            ConsoleStyle::line => new StatusLineHeartbeat($target),
            ConsoleStyle::log => new LogHeartbeat($target),
        };
    }
}
