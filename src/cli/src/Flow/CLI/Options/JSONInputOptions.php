<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class JSONInputOptions
{
    public function configure(Command $command): void
    {
        $command->addOption(
            'input-json-pointer',
            null,
            InputOption::VALUE_REQUIRED,
            'JSON Pointer to a subtree from which schema should be extracted',
            null,
        )->addOption(
            'input-json-pointer-column-name',
            null,
            InputOption::VALUE_NONE,
            'When set, JSON Pointer will be used as a column name in the schema',
            null,
        );
    }
}
