<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait StatisticsOptions
{
    private function addStatisticsOptions(Command $command) : void
    {
        $command
            ->addOption('stats-schema', null, InputOption::VALUE_OPTIONAL, 'Prints schema of executed data transformation pipeline.', false)
            ->addOption('stats-columns', null, InputOption::VALUE_OPTIONAL, 'Prints number of rows in dataset.', false);
    }
}
