<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Helper\Helper;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;
use Symfony\Component\Console\Terminal;

use function array_fill;
use function array_filter;
use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function implode;
use function max;
use function min;
use function number_format;
use function sprintf;
use function str_pad;
use function str_repeat;

final class TaskListHeartbeat implements ConsoleHeartbeat
{
    private const int GRAPH_WIDTH = 90;

    private const int LABEL_WIDTH = 10;

    private const int SUBJECT_WIDTH = 24;

    private const int MIN_DOTS = 3;

    private int $redrawableLines = 0;

    private int $frame = 0;

    private int $frozenLines = 0;

    private ?Pulse $last = null;

    /**
     * @var array<string, array{float, ?string, string}> the end, exact and short DONE line of each ended entry
     */
    private array $printed = [];

    private bool $short = false;

    public function __construct(
        private readonly StreamOutput $output,
        private readonly ?ThroughputGraph $graph = null,
    ) {}

    public function beat(Pulse $pulse): void
    {
        if ($this->output->isQuiet()) {
            return;
        }

        // a first beat after an earlier one is a new run: the lines of the run before stay where they are
        if ($pulse->beat === 1 && $this->last !== null) {
            $this->redrawableLines = 0;
            $this->frozenLines = 0;
            $this->printed = [];
            $this->short = false;
        }

        $this->graph?->recorded($pulse);
        $this->frame++;
        $this->last = $pulse;
        $width = min(self::MAX_WIDTH, (new Terminal())->getWidth()) - 1;
        $exact = $this->lines($pulse, $width, false);
        $this->short = $exact === null;

        $this->redraw(
            $exact ?? $this->lines($pulse, $width, true),
            array_map(static fn(StageProgress $stage): bool => $stage->endedAtSeconds !== null, $pulse->stages),
        );
    }

    public function finish(): void
    {
        $last = $this->last;

        if ($this->output->isQuiet() || $last === null) {
            return;
        }

        $done = [];

        foreach ($last->stages as $position => $stage) {
            $key = StageLine::key($stage, $position);

            if (array_key_exists($key, $this->printed)) {
                $done[] = $this->short ? $this->printed[$key][2] : $this->printed[$key][1] ?? $this->printed[$key][2];
            }
        }

        // a failed run leaves its DONE lines; the open ones go
        $this->redraw($done, array_fill(0, count($done), true));

        if (StageLine::readCompleted($last)) {
            $this->output->writeln('');
            $this->output->writeln($this->summary($last, min(self::MAX_WIDTH, (new Terminal())->getWidth()) - 1));
            $this->output->writeln('');
        }

        $this->redrawableLines = 0;
        $this->frozenLines = 0;
        $this->last = null;
        $this->printed = [];
        $this->frame = 0;
        $this->short = false;
    }

    private function graph(StageProgress $stage, int $position, int $width): ?string
    {
        // $width leaves the last column free, and the sparkline needs GRAPH_WIDTH columns of the terminal
        return $this->graph === null || ($width + 1) < self::GRAPH_WIDTH
            ? null
            : $this->graph->sparkline($stage, $position);
    }

    /**
     * @return ($short is true ? list<string> : null|list<string>) null when a line needs the short count
     */
    private function lines(Pulse $pulse, int $width, bool $short): ?array
    {
        $lines = [];

        foreach ($pulse->stages as $position => $stage) {
            $line = new StageLine($stage, $pulse);
            $rate = $line->rate();
            $key = StageLine::key($stage, $position);

            if (!$line->started()) {
                $skipped = $stage->endedAtSeconds !== null;
                $drawn = $this->notStartedLine(
                    $skipped ? '<fg=gray>–</>' : '<fg=gray>○</>',
                    $line,
                    $width,
                    $skipped ? '<fg=gray>skipped</>' : '<fg=gray>waiting</>',
                );

                if ($skipped) {
                    $this->printed[$key] = [$stage->endedAtSeconds, $drawn, $drawn];
                } else {
                    unset($this->printed[$key]);
                }
            } elseif ($stage->endedAtSeconds === null) {
                unset($this->printed[$key]);
                $icon = '<fg=cyan>' . self::SPINNER[$this->frame % count(self::SPINNER)] . '</>';
                $right = [
                    'graph' => $this->graph($stage, $position, $width),
                    'rate' => $rate === null ? null : '<fg=yellow>' . $rate . '</>',
                    'memory' => $this->output->isVerbose()
                        ? '<fg=gray>' . StageLine::memory($pulse->memoryBytes) . '</>'
                        : null,
                    'time' => (string) $line->duration(),
                ];
                $drawn = $short
                    ? $this->shortLine($icon, $line, $width, $right)
                    : $this->exactLine($icon, $line, $width, $right);
            } else {
                // an entry that opened again and ended later shows the numbers of its new end
                if (($this->printed[$key][0] ?? null) !== $stage->endedAtSeconds) {
                    $right = [
                        'graph' => $this->graph($stage, $position, $width),
                        'rate' => $rate === null ? null : '<fg=gray>' . $rate . '</>',
                        'memory' => null,
                        'time' => '<fg=gray>' . (string) $line->duration() . '</> <fg=green;options=bold>DONE</>',
                    ];
                    $this->printed[$key] = [
                        $stage->endedAtSeconds,
                        $this->exactLine('<fg=green>✔</>', $line, $width, $right),
                        $this->shortLine('<fg=green>✔</>', $line, $width, $right),
                    ];
                }

                $drawn = $short ? $this->printed[$key][2] : $this->printed[$key][1];
            }

            if ($drawn === null) {
                return null;
            }

            $lines[] = $drawn;
        }

        return $lines;
    }

    /**
     * @param array{graph: ?string, rate: ?string, memory: ?string, time: string} $right
     */
    private function exactLine(string $icon, StageLine $line, int $width, array $right): ?string
    {
        $label = $this->label($icon, $line);
        $count = $line->exactCount() . $this->percent($line);

        return $this->fittedDroppingColumns(
            $label . $this->subject($line->subject($this->subjectRoom($label, $count, $right, $width))) . $count,
            $right,
            $width,
        );
    }

    /**
     * @param array{graph: ?string, rate: ?string, memory: ?string, time: string} $right
     */
    private function shortLine(string $icon, StageLine $line, int $width, array $right): string
    {
        $label = $this->label($icon, $line);
        $count = $line->shortCount() . $this->percent($line);
        $subject = $line->subject($this->subjectRoom($label, $count, $right, $width));
        $drawn = $this->fittedDroppingColumns($label . $this->subject($subject) . $count, $right, $width);

        if ($drawn !== null) {
            return $drawn;
        }

        $columns = ['time' => $right['time']];

        if ($subject !== null) {
            $room = $width - $this->visible($label . $count) - $this->visible($right['time']) - 2 - self::MIN_DOTS - 2;
            $cut = $line->subject($room);
            $drawn = $cut === null ? null : $this->fitted($label . $this->subject($cut) . $count, $columns, $width);

            if ($drawn !== null) {
                return $drawn;
            }
        }

        return $this->fitted($label . $count, $columns, $width) ?? $label . $count . ' ' . $right['time'];
    }

    /**
     * @param array{graph: ?string, rate: ?string, memory: ?string, time: string} $right
     */
    private function fittedDroppingColumns(string $left, array $right, int $width): ?string
    {
        foreach (['', 'memory', 'rate', 'graph'] as $dropped) {
            if ($dropped !== '') {
                $right[$dropped] = null;
            }

            $drawn = $this->fitted($left, $right, $width);

            if ($drawn !== null) {
                return $drawn;
            }
        }

        return null;
    }

    private function notStartedLine(string $icon, StageLine $line, int $width, string $state): string
    {
        // no count follows, so the label is padded only to align a subject, and one space leads to the dots
        $name = '  ' . $icon . ' <options=bold>' . OutputFormatter::escape($line->label()) . '</>';
        $columns = ['time' => $state];
        $subject = $line->subject($this->subjectRoom($this->label($icon, $line), '', $columns, $width));

        return (
            (
                $subject === null
                    ? null
                    : $this->fitted(
                        $this->label($icon, $line) . '<fg=gray>' . OutputFormatter::escape($subject) . '</>',
                        $columns,
                        $width,
                    )
            )
            ?? $this->fitted($name, $columns, $width)
            ?? $name . ' ' . $state
        );
    }

    // the first form that fits: exact counts, short counts, without the files, without the peak
    private function summary(Pulse $last, int $width): string
    {
        $files = StageLine::filesWritten($last);
        $rows = StageLine::unitName($last->rowsRead, ProgressUnit::rows);
        $written = $files === null
            ? null
            : '<fg=gray>' . StageLine::counted($files, ProgressUnit::files) . ' written</>';
        $time = ' in <options=bold>' . StageLine::seconds($last->elapsedSeconds) . '</>';
        $peak = '<fg=gray>peak ' . StageLine::memory($last->peakMemoryBytes) . '</>';
        $forms = [
            [number_format($last->rowsRead) . ' ' . $rows, $peak, $written],
            [StageLine::human($last->rowsRead) . ' ' . $rows, $peak, $written],
            [StageLine::human($last->rowsRead) . ' ' . $rows, $peak, null],
            [StageLine::human($last->rowsRead) . ' ' . $rows, null, null],
        ];
        $lines = array_map(
            static fn(array $form): string => '  <bg=green;fg=black> DONE </> <options=bold>'
            . $form[0]
            . '</>'
            . $time
            . implode('', array_map(
                static fn(string $part): string => '<fg=gray> · </>' . $part,
                array_filter([$form[1], $form[2]], static fn(?string $part): bool => $part !== null),
            )),
            $forms,
        );

        foreach ($lines as $line) {
            if ($this->visible($line) <= $width) {
                return $line;
            }
        }

        return $lines[count($lines) - 1];
    }

    private function label(string $icon, StageLine $line): string
    {
        return (
            '  '
            . $icon
            . ' <options=bold>'
            . OutputFormatter::escape(str_pad($line->label(), self::LABEL_WIDTH))
            . '</>'
        );
    }

    private function percent(StageLine $line): string
    {
        // padded to three digits, so it lines up in the column of every line
        return $line->percent() === null ? '' : sprintf(' <fg=gray>%3d%%</>', $line->percent());
    }

    private function subject(?string $subject): string
    {
        return $subject === null ? '' : '<fg=gray>' . OutputFormatter::escape($subject) . '</>  ';
    }

    /**
     * @param array<string, ?string> $columns
     */
    private function fitted(string $left, array $columns, int $width): ?string
    {
        $right = $this->joined($columns);
        $dots = $width - $this->visible($left) - $this->visible($right) - 2;

        return $dots >= self::MIN_DOTS ? $left . ' <fg=gray>' . str_repeat('.', $dots) . '</> ' . $right : null;
    }

    /**
     * @param array<string, ?string> $columns
     */
    private function joined(array $columns): string
    {
        return implode('<fg=gray> · </>', array_filter(
            $columns,
            static fn(?string $column): bool => $column !== null && $column !== '',
        ));
    }

    /**
     * @param array<string, ?string> $columns
     */
    private function subjectRoom(string $label, string $count, array $columns, int $width): int
    {
        return max(
            self::SUBJECT_WIDTH,
            $width - $this->visible($label . $count) - $this->visible($this->joined($columns)) - 2 - self::MIN_DOTS - 2,
        );
    }

    /**
     * @param list<string> $lines the block, in list order
     * @param list<bool> $done which lines are DONE lines, whose text no longer changes
     */
    private function redraw(array $lines, array $done): void
    {
        $from = $this->frozenLines;
        $reachable = max(1, (new Terminal())->getHeight() - 1);

        while ((count($lines) - $this->frozenLines) > $reachable && ($done[$this->frozenLines] ?? false)) {
            $this->frozenLines++;
        }

        if ($this->redrawableLines > 0) {
            $this->output->write(
                sprintf("\x1B[%dA\x1B[0J", $this->redrawableLines),
                false,
                OutputInterface::OUTPUT_RAW,
            );
        }

        foreach (array_slice($lines, $from) as $line) {
            $this->output->writeln($line);
        }

        $this->redrawableLines = count($lines) - $this->frozenLines;
    }

    private function visible(string $text): int
    {
        return Helper::width(Helper::removeDecoration($this->output->getFormatter(), $text));
    }
}
