<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;

final readonly class XMLInputOptions
{
    public function configure(Command $command): void
    {
        $command->addOption(
            'input-xml-node-path',
            null,
            InputOption::VALUE_REQUIRED,
            'XML node path to a subtree from which schema should be extracted, for example /root/element <info>This is not xpath, just a node names separated by slash</info>',
        )->addOption('input-xml-buffer-size', null, InputOption::VALUE_REQUIRED, 'XML buffer size in bytes');
    }
}
