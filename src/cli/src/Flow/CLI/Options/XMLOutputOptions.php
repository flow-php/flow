<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class XMLOutputOptions
{
    public function configure(Command $command): void
    {
        $command
            ->addOption(
                'output-xml-root-element',
                null,
                InputOption::VALUE_REQUIRED,
                'Root element name in XML output',
                null,
            )
            ->addOption(
                'output-xml-row-element',
                null,
                InputOption::VALUE_REQUIRED,
                'Row element name in XML output',
                null,
            )
            ->addOption(
                'output-xml-attribute-prefix',
                null,
                InputOption::VALUE_REQUIRED,
                'Attribute prefix in XML output',
                null,
            )
            ->addOption(
                'output-xml-date-time-format',
                null,
                InputOption::VALUE_REQUIRED,
                'Date time format used in XML output',
                null,
            );
    }
}
