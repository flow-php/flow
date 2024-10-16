<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Flow\ETL\Config;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

trait ConfigOptions
{
    private function addConfigOptions(Command $command, string $prefix = '') : void
    {
        $command
            ->addOption($prefix . 'config', null, InputOption::VALUE_REQUIRED, 'Path to a local php file that MUST return instance of: ' . Config::class);
    }
}
