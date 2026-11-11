<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Context;

use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

use function fopen;
use function getenv;
use function preg_replace;
use function putenv;
use function rewind;
use function stream_get_contents;

final class TerminalContext
{
    public readonly false|string $columnsBefore;

    public readonly false|string $linesBefore;

    public function __construct(int $columns, int $lines = 50)
    {
        $this->columnsBefore = getenv('COLUMNS');
        $this->linesBefore = getenv('LINES');
        $this->columns($columns);
        $this->lines($lines);
    }

    public function columns(int $columns): void
    {
        putenv('COLUMNS=' . $columns);
    }

    public function lines(int $lines): void
    {
        putenv('LINES=' . $lines);
    }

    public function restore(): void
    {
        putenv($this->columnsBefore === false ? 'COLUMNS' : 'COLUMNS=' . $this->columnsBefore);
        putenv($this->linesBefore === false ? 'LINES' : 'LINES=' . $this->linesBefore);
    }

    /**
     * A command's output on two memory streams: stdout is the console itself, stderr its error output.
     *
     * @return ConsoleOutputInterface&StreamOutput
     */
    public function console(): StreamOutput
    {
        return new class($this->stream(), $this->stream()) extends StreamOutput implements ConsoleOutputInterface {
            public function __construct(
                StreamOutput $stdout,
                private OutputInterface $stderr,
            ) {
                parent::__construct($stdout->getStream(), $stdout->getVerbosity(), $stdout->isDecorated());
            }

            public function getErrorOutput(): OutputInterface
            {
                return $this->stderr;
            }

            public function setErrorOutput(OutputInterface $error): void
            {
                $this->stderr = $error;
            }

            public function section(): ConsoleSectionOutput
            {
                $sections = [];

                return new ConsoleSectionOutput(
                    $this->getStream(),
                    $sections,
                    $this->getVerbosity(),
                    true,
                    new OutputFormatter(),
                );
            }
        };
    }

    public function section(): ConsoleSectionOutput
    {
        $sections = [];

        return new ConsoleSectionOutput(
            fopen('php://memory', 'w+b'),
            $sections,
            OutputInterface::VERBOSITY_NORMAL,
            true,
            new OutputFormatter(),
        );
    }

    public function stream(int $verbosity = OutputInterface::VERBOSITY_NORMAL): StreamOutput
    {
        return new StreamOutput(fopen('php://memory', 'w+b'), $verbosity, true);
    }

    public function raw(StreamOutput $output): string
    {
        rewind($output->getStream());

        return (string) stream_get_contents($output->getStream());
    }

    public function text(StreamOutput $output): string
    {
        return (string) preg_replace('/\x1B\[[0-9;]*[A-Za-z]/', '', $this->raw($output));
    }
}
