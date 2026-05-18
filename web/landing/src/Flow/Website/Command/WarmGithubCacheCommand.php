<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Flow\Website\Service\Github;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function sprintf;

#[AsCommand(name: 'app:github:warm-cache', description: 'Fetch GitHub contributors and warm the cache')]
final class WarmGithubCacheCommand extends Command
{
    public function __construct(
        private readonly Github $github,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $contributors = $this->github->contributors();

        $io->success(sprintf('Fetched %d contributors', count($contributors)));

        return Command::SUCCESS;
    }
}
