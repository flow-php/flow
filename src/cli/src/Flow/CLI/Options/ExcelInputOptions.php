<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class ExcelInputOptions
{
    public function configure(Command $command): void
    {
        $command->addOption(
            'input-excel-header',
            null,
            InputOption::VALUE_OPTIONAL,
            'When set, Excel header will be used as a schema',
        )->addOption(
            'input-excel-sheet-name',
            null,
            InputOption::VALUE_REQUIRED,
            'When set, Excel sheet name will be selected for reading',
        );
    }
}
