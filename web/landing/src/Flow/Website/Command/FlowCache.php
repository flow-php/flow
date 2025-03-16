<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Flow\Website\Service\Github;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:cache:refresh', description: 'Refresh Flow Cache')]
final class FlowCache extends Command
{
    public function __construct(private readonly Github $github)
    {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        // $style->note('Flow Version: ' . $this->github->version('flow-php/flow', true));

        return Command::SUCCESS;
    }
}
