<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class DbConnectionOptions
{
    public const string HELP = "Database connection string can be passed through <info>FLOW_DB_CONNECTION_STRING</info> environment variable, otherwise command will ask for it.\n<info>--db-connection-file</info> option takes priority over <info>FLOW_DB_CONNECTION_STRING</info> environment.";

    public function configure(Command $command): void
    {
        $command->addOption(
            'db-connection-file',
            'c',
            InputOption::VALUE_REQUIRED,
            'Path to file that returns and instance of \\Doctrine\\DBAL\\Connection',
            null,
        );
    }
}
