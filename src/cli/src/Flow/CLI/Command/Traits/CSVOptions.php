<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait CSVOptions
{
    private function addCSOutputOptions(Command $command) : void
    {
        $command
            ->addOption('output-csv-header', null, InputOption::VALUE_OPTIONAL, 'When set, CSV header will be used as a schema')
            ->addOption('output-csv-new-line-separator', null, InputOption::VALUE_REQUIRED, 'When set, empty CSV values will be treated as NULL values')
            ->addOption('output-csv-separator', null, InputOption::VALUE_REQUIRED, 'CSV separator character')
            ->addOption('output-csv-enclosure', null, InputOption::VALUE_REQUIRED, 'CSV enclosure character')
            ->addOption('output-csv-escape', null, InputOption::VALUE_REQUIRED, 'CSV escape character')
            ->addOption('output-csv-date-time-format', null, InputOption::VALUE_REQUIRED, 'DateTime format for CSV output');
    }

    private function addCSVInputOptions(Command $command) : void
    {
        $command
            ->addOption('input-csv-header', null, InputOption::VALUE_OPTIONAL, 'When set, CSV header will be used as a schema')
            ->addOption('input-csv-empty-to-null', null, InputOption::VALUE_OPTIONAL, 'When set, empty CSV values will be treated as NULL values')
            ->addOption('input-csv-separator', null, InputOption::VALUE_REQUIRED, 'CSV separator character')
            ->addOption('input-csv-enclosure', null, InputOption::VALUE_REQUIRED, 'CSV enclosure character')
            ->addOption('input-csv-escape', null, InputOption::VALUE_REQUIRED, 'CSV escape character');
    }
}
