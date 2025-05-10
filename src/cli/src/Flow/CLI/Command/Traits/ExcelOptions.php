<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait ExcelOptions
{
    private function addExcelInputOptions(Command $command) : void
    {
        $command
            ->addOption('input-excel-header', null, InputOption::VALUE_OPTIONAL, 'When set, Excel header will be used as a schema')
            ->addOption('input-excel-sheet-name', null, InputOption::VALUE_REQUIRED, 'When set, Excel sheet name will be selected for reading')
            ->addOption('input-excel-offset', null, InputOption::VALUE_REQUIRED, 'Offset to start reading from');
    }
}
