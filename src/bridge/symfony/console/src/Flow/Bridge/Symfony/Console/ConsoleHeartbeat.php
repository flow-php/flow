<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Runtime\Heartbeat\Heartbeat;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Symfony\Component\Console\Cursor;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Helper\Helper;
use Symfony\Component\Console\Helper\ProgressIndicator;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Terminal;

use function implode;
use function intdiv;
use function max;
use function number_format;
use function sprintf;

final class ConsoleHeartbeat implements Heartbeat
{
    private const int INDICATOR_AND_LAST_COLUMN = 4;

    private const array TIME_UNITS = ['d' => 86_400, 'h' => 3_600, 'min' => 60, 's' => 1];

    private ?ProgressIndicator $indicator = null;

    private readonly OutputInterface $output;

    public function __construct(OutputInterface $output)
    {
        $this->output = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;
    }

    public function beat(Pulse $pulse): void
    {
        $line = sprintf(
            '%s: %s %s · %s read · %s (peak %s) · %s',
            $pulse->stage === '' ? 'starting' : $pulse->stage,
            number_format($pulse->stageRows),
            ($pulse->stage === ProgressStages::COMMIT ? 'file operation' : 'row')
            . ($pulse->stageRows === 1 ? '' : 's'),
            number_format($pulse->rowsRead),
            Helper::formatMemory($pulse->memoryBytes),
            Helper::formatMemory($pulse->peakMemoryBytes),
            self::elapsed($pulse->elapsedSeconds),
        );
        // a line that wraps leaves its first row behind on every redraw
        $message = OutputFormatter::escape(
            $this->output->isDecorated() && !$this->output instanceof ConsoleSectionOutput
                ? Helper::substr($line, 0, max(1, (new Terminal())->getWidth() - self::INDICATOR_AND_LAST_COLUMN))
                : $line,
        );

        if ($this->indicator === null) {
            $this->indicator = new ProgressIndicator(
                $this->output,
                $this->output->isDecorated() ? 'normal' : 'normal_no_ansi',
            );
            $this->indicator->start($message);

            return;
        }

        $this->indicator->setMessage($message);
        $this->indicator->advance();
    }

    public function finish(): void
    {
        if ($this->indicator !== null && $this->output->isDecorated()) {
            if ($this->output instanceof ConsoleSectionOutput) {
                $this->output->clear();
            } else {
                (new Cursor($this->output))
                    ->moveToColumn(1)
                    ->clearLine();
            }
        }

        $this->indicator = null;
    }

    private static function elapsed(int $seconds): string
    {
        $shown = [];
        $positions = 0;

        foreach (self::TIME_UNITS as $unit => $unitSeconds) {
            $count = intdiv($seconds, $unitSeconds);
            $seconds %= $unitSeconds;

            if ($positions === 0 && $count === 0) {
                continue;
            }

            if ($count > 0) {
                $shown[] = "{$count} {$unit}";
            }

            if (++$positions === 2) {
                break;
            }
        }

        return $shown === [] ? '0 s' : implode(', ', $shown);
    }
}
