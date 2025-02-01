<?php

declare(strict_types=1);

namespace Flow\CLI\Style;

use Symfony\Component\Console\Cursor;
use Symfony\Component\Console\Formatter\{OutputFormatterStyle};
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\{SymfonyStyle};

final class FlowStyle extends SymfonyStyle
{
    public function __construct(InputInterface $input, private readonly OutputInterface $output)
    {
        parent::__construct($input, $output);

        $output->getFormatter()->setStyle('blue-block', new OutputFormatterStyle('white', 'blue'));

        $output->getFormatter()->setStyle('flow-orange-01', new OutputFormatterStyle('#FF5547', null, ['bold', 'blink']));
        $output->getFormatter()->setStyle('flow-blue-01', new OutputFormatterStyle('#806DFE', null, ['bold', 'blink']));
        $output->getFormatter()->setStyle('flow-blue-02', new OutputFormatterStyle('#5945D8', null, ['bold', 'blink']));
        $output->getFormatter()->setStyle('flow-blue-03', new OutputFormatterStyle('#4026AC', null, ['bold', 'blink']));
    }

    public function clear() : void
    {
        (new Cursor($this->output))->clearOutput();
    }
}
