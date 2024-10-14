<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait CSVExtractorOptions
{
    private function addCSVOptions(Command $command) : void
    {
        $command
            ->addOption('csv-header', null, InputOption::VALUE_OPTIONAL, 'When set, CSV header will be used as a schema')
            ->addOption('csv-empty-to-null', null, InputOption::VALUE_OPTIONAL, 'When set, empty CSV values will be treated as NULL values')
            ->addOption('csv-separator', null, InputOption::VALUE_OPTIONAL, 'CSV separator character')
            ->addOption('csv-enclosure', null, InputOption::VALUE_OPTIONAL, 'CSV enclosure character')
            ->addOption('csv-escape', null, InputOption::VALUE_OPTIONAL, 'CSV escape character');
    }
}
