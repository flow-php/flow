<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'test:failing', description: 'A failing test command for telemetry')]
final class FailingCommand extends Command
{
    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        return Command::FAILURE;
    }
}
