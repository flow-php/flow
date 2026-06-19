<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command;

use Flow\Telemetry\Telemetry;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'test:logging', description: 'Emits one log per severity for console output tests')]
final class LoggingCommand extends Command
{
    public function __construct(
        private readonly Telemetry $telemetry,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $logger = $this->telemetry->logger('test');
        $logger->error('error-message');
        $logger->warn('warn-message');
        $logger->info('info-message');
        $logger->debug('debug-message');
        $logger->trace('trace-message');

        return Command::SUCCESS;
    }
}
