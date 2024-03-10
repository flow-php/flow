<?php

declare(strict_types=1);

namespace Flow\ParquetViewer;

use Flow\ParquetViewer\Command\ReadDataCommand;
use Flow\ParquetViewer\Command\ReadMetadataCommand;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\CompleteCommand;
use Symfony\Component\Console\Command\DumpCompletionCommand;
use Symfony\Component\Console\Command\HelpCommand;
use Symfony\Component\Console\Command\ListCommand;

final class Parquet extends Application
{
    protected function getDefaultCommands() : array
    {
        return [
            new HelpCommand(),
            new ListCommand(),
            new CompleteCommand(),
            new DumpCompletionCommand(),
            new ReadMetadataCommand(),
            new ReadDataCommand(),
        ];
    }
}
