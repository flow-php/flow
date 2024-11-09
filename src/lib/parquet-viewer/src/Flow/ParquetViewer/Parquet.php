<?php

declare(strict_types=1);

namespace Flow\ParquetViewer;

use Flow\ParquetViewer\Command\{ReadDDLCommand, ReadDataCommand, ReadMetadataCommand};
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\{CompleteCommand, DumpCompletionCommand, HelpCommand, ListCommand};

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
            new ReadDDLCommand(),
        ];
    }
}
