<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait JSONOptions
{
    private function addJSONInputOptions(Command $command) : void
    {
        $command
            ->addOption('input-json-pointer', null, InputOption::VALUE_REQUIRED, 'JSON Pointer to a subtree from which schema should be extracted', null)
            ->addOption('input-json-pointer-entry-name', null, InputOption::VALUE_NONE, 'When set, JSON Pointer will be used as an entry name in the schema', null);
    }
}
