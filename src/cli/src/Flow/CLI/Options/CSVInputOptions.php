<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class CSVInputOptions
{
    public function configure(Command $command): void
    {
        $command
            ->addOption(
                'input-csv-header',
                null,
                InputOption::VALUE_OPTIONAL,
                'When set, CSV header will be used as a schema',
            )
            ->addOption(
                'input-csv-empty-to-null',
                null,
                InputOption::VALUE_OPTIONAL,
                'When set, empty CSV values will be treated as NULL values',
            )
            ->addOption('input-csv-separator', null, InputOption::VALUE_REQUIRED, 'CSV separator character')
            ->addOption('input-csv-enclosure', null, InputOption::VALUE_REQUIRED, 'CSV enclosure character')
            ->addOption('input-csv-escape', null, InputOption::VALUE_REQUIRED, 'CSV escape character');
    }
}
