<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait ParquetExtractorOptions
{
    private function addParquetOptions(Command $command) : void
    {
        $command
            ->addOption('parquet-columns', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns to read from parquet file')
            ->addOption('parquet-offset', null, InputOption::VALUE_REQUIRED, 'Offset to start reading from');
    }
}
