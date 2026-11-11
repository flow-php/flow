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

        if (StageLine::runCompleted($last)) {
            $files = StageLine::filesWritten($last);
            $this->output->writeln('');
            $this->output->writeln(sprintf(
                '  <bg=green;fg=black> DONE </> <options=bold>%s</> in <options=bold>%s</><fg=gray> · peak %s%s</>',
                StageLine::counted($last->rowsRead, ProgressUnit::rows),
                StageLine::seconds($last->elapsedSeconds),
                StageLine::memory($last->peakMemoryBytes),
                $files === null ? '' : ' · ' . StageLine::counted($files, ProgressUnit::files) . ' written',
            ));
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

            if ($stage->endedAtSeconds === null) {
                unset($this->printed[$key]);
                $icon = '<fg=cyan>' . self::SPINNER[$this->frame % count(self::SPINNER)] . '</>';
                $right = [
                    'graph' => $this->graph($stage, $position, $width),
                    'rate' => $rate === null ? null : '<fg=yellow>' . $rate . '</>',
                    'memory' => $this->output->isVerbose()
                        ? '<fg=gray>' . StageLine::memory($pulse->memoryBytes) . '</>'
                        : null,
                    'time' => $line->duration(),
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
                        'time' => '<fg=gray>' . $line->duration() . '</> <fg=green;options=bold>DONE</>',
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
        return $this->fittedDroppingColumns(
            $this->label($icon, $line) . $this->location($line->location(self::LOCATION_WIDTH)) . $line->exactCount()
                . $this->percent($line),
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
        $location = $line->location(self::LOCATION_WIDTH);
        $count = $line->shortCount() . $this->percent($line);
        $drawn = $this->fittedDroppingColumns($label . $this->location($location) . $count, $right, $width);

        if ($drawn !== null) {
            return $drawn;
        }

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
