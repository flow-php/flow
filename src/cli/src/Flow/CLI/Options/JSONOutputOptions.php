<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class JSONOutputOptions
{
    public function configure(Command $command): void
    {
        $command->addOption(
            'output-json-date-time-format',
            null,
            InputOption::VALUE_REQUIRED,
            'Date time format used in JSON output',
            null,
        )->addOption(
            'output-json-rows-in-new-line',
            null,
            InputOption::VALUE_NONE,
            'When set, each row will be printed in a new line',
            null,
        );
    }
}
