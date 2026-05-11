<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:postgresql:session:purge', description: 'Purges sessions from the PostgreSQL session store.')]
final class SessionPurgeCommand extends Command
{
    public function __construct(
        private readonly FlowPostgreSqlSessionHandler $handler,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('expired', null, InputOption::VALUE_NONE, 'Purge only expired sessions (default).')->addOption(
            'all',
            null,
            InputOption::VALUE_NONE,
            'Purge ALL sessions, including active ones.',
        );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $all = (bool) $input->getOption('all');
        $expired = (bool) $input->getOption('expired');

        if ($all && $expired) {
            $io->error('Options --expired and --all are mutually exclusive.');

            return Command::INVALID;
        }

        if ($all) {
            $this->handler->purgeAll();
            $io->success('Purged all sessions.');

            return Command::SUCCESS;
        }

        $count = $this->handler->purgeExpired();
        $io->success(\sprintf('Purged %d expired session(s).', $count));

        return Command::SUCCESS;
    }
}
