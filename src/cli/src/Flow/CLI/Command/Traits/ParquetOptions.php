<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait ParquetOptions
{
    private function addParquetInputOptions(Command $command) : void
    {
        $command
            ->addOption('input-parquet-columns', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns to read from parquet file')
            ->addOption('input-parquet-offset', null, InputOption::VALUE_REQUIRED, 'Offset to start reading from');
    }
}
