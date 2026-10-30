<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class ParquetInputOptions
{
    public function configure(Command $command): void
    {
        $command->addOption(
            'input-parquet-columns',
            null,
            InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY,
            'Columns to read from parquet file',
        );
    }
}
