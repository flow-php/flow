<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

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

    private const int LOCATION_WIDTH = 24;

    private const int MIN_DOTS = 3;

    private int $redrawableLines = 0;

    private int $frame = 0;

    private int $frozenLines = 0;

    private ?Pulse $last = null;

    /**
     * @var array<string, array{float, string}> the end and the DONE line of every entry that is ended, by key
     */
    private array $printed = [];

    public function __construct(
        private readonly StreamOutput $output,
        private readonly ?ThroughputGraph $graph = null,
    ) {}

    public function beat(Pulse $pulse): void
    {
        if ($this->output->isQuiet()) {
            return;
        }

        $this->graph?->recorded($pulse);
        $this->frame++;
        $this->last = $pulse;
        $width = min(self::MAX_WIDTH, (new Terminal())->getWidth()) - 1;
        $lines = [];
        $done = [];

        foreach ($pulse->stages as $position => $stage) {
            $line = new StageLine($stage, $pulse);
            $rate = $line->rate();

            if ($stage->endedAtSeconds === null) {
                unset($this->printed[StageLine::key($stage, $position)]);
                $done[] = false;
                $lines[] = $this->line(
                    '<fg=cyan>' . self::SPINNER[$this->frame % count(self::SPINNER)] . '</>',
                    $line,
                    $width,
                    [
                        'graph' => $this->graph($stage, $position, $width),
                        'rate' => $rate === null ? null : '<fg=yellow>' . $rate . '</>',
                        'memory' => $this->output->isVerbose()
                            ? '<fg=gray>' . StageLine::memory($pulse->memoryBytes) . '</>'
                            : null,
                        'time' => $line->duration(),
                    ],
                );

                continue;
            }

            $key = StageLine::key($stage, $position);

            // an entry that opened again and ended later shows the numbers of its new end
            if (($this->printed[$key][0] ?? null) !== $stage->endedAtSeconds) {
                $this->printed[$key] = [$stage->endedAtSeconds, $this->line('<fg=green>✔</>', $line, $width, [
                    'graph' => $this->graph($stage, $position, $width),
                    'rate' => $rate === null ? null : '<fg=gray>' . $rate . '</>',
                    'memory' => null,
                    'time' => '<fg=gray>' . $line->duration() . '</> <fg=green;options=bold>DONE</>',
                ])];
            }

            $done[] = true;
            $lines[] = $this->printed[$key][1];
        }

        $this->redraw($lines, $done);
    }

    public function finish(): void
    {
        $last = $this->last;

        if ($this->output->isQuiet() || $last === null) {
            return;
        }

        $done = [];

        foreach ($last->stages as $position => $stage) {
            if (array_key_exists(StageLine::key($stage, $position), $this->printed)) {
                $done[] = $this->printed[StageLine::key($stage, $position)][1];
            }
        }

        // a failed run leaves its DONE lines; the open ones go
        $this->redraw($done, array_fill(0, count($done), true));

        if (StageLine::runCompleted($last)) {
            $files = StageLine::filesWritten($last);
            $this->output->writeln('');
            $this->output->writeln(sprintf(
                '  <bg=green;fg=black> DONE </> <options=bold>%s rows</> in <options=bold>%s</><fg=gray> · peak %s%s</>',
                number_format($last->rowsRead),
                StageLine::seconds($last->elapsedSeconds),
                StageLine::memory($last->peakMemoryBytes),
                $files === null ? '' : ' · ' . number_format($files) . ' files written',
            ));
            $this->output->writeln('');
        }

        $this->redrawableLines = 0;
        $this->frozenLines = 0;
        $this->last = null;
        $this->printed = [];
        $this->frame = 0;
    }

    private function graph(StageProgress $stage, int $position, int $width): ?string
    {
        // $width leaves the last column free, and the sparkline needs GRAPH_WIDTH columns of the terminal
        return $this->graph === null || ($width + 1) < self::GRAPH_WIDTH
            ? null
            : $this->graph->sparkline($stage, $position);
    }

    /**
     * @param array{graph: ?string, rate: ?string, memory: ?string, time: string} $right
     */
    private function line(string $icon, StageLine $line, int $width, array $right): string
    {
        $label =
            '  '
            . $icon
            . ' <options=bold>'
            . OutputFormatter::escape(str_pad($line->label(), self::LABEL_WIDTH))
            . '</>';
        $location = $line->location(self::LOCATION_WIDTH);
        // the percent is padded to three digits, so it lines up in the column of every line
        $percent = $line->percent() === null ? '' : sprintf(' <fg=gray>%3d%%</>', $line->percent());

        foreach ([$line->exactCount() . $percent, $line->shortCount() . $percent] as $count) {
            $columns = $right;

            foreach (['', 'memory', 'rate', 'graph'] as $dropped) {
                if ($dropped !== '') {
                    $columns[$dropped] = null;
                }

                $drawn = $this->fitted($label . $this->location($location) . $count, $columns, $width);

                if ($drawn !== null) {
                    return $drawn;
                }
            }
        }

        $count = $line->shortCount() . $percent;
        $columns = ['time' => $right['time']];

        if ($location !== null) {
            $room = $width - $this->visible($label . $count) - $this->visible($right['time']) - 2 - self::MIN_DOTS - 2;
            $cut = $line->location($room);
            $drawn = $cut === null ? null : $this->fitted($label . $this->location($cut) . $count, $columns, $width);

            if ($drawn !== null) {
                return $drawn;
            }
        }

        return $this->fitted($label . $count, $columns, $width) ?? $label . $count . ' ' . $right['time'];
    }

    private function location(?string $location): string
    {
        return $location === null ? '' : '<fg=gray>' . OutputFormatter::escape($location) . '</>  ';
    }

    /**
     * @param array<string, ?string> $columns
     */
    private function fitted(string $left, array $columns, int $width): ?string
    {
        $right = implode('<fg=gray> · </>', array_filter(
            $columns,
            static fn(?string $column): bool => $column !== null && $column !== '',
        ));
        $dots = $width - $this->visible($left) - $this->visible($right) - 2;

        return $dots >= self::MIN_DOTS ? $left . ' <fg=gray>' . str_repeat('.', $dots) . '</> ' . $right : null;
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
