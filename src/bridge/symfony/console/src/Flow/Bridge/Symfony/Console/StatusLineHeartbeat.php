<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Runtime\Heartbeat\Pulse;
use Symfony\Component\Console\Cursor;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Helper\Helper;
use Symfony\Component\Console\Helper\ProgressIndicator;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Terminal;

use function array_filter;
use function array_map;
use function array_slice;
use function count;
use function implode;
use function max;
use function min;
use function number_format;
use function sprintf;

final class StatusLineHeartbeat implements ConsoleHeartbeat
{
    private const int INDICATOR_AND_LAST_COLUMN = 4;

    private const int LOCATION_WIDTH = 16;

    private ?ProgressIndicator $indicator = null;

    private ?Pulse $last = null;

    public function __construct(
        private readonly OutputInterface $output,
    ) {}

    public function beat(Pulse $pulse): void
    {
        $this->last = $pulse;
        $message = $this->message($pulse);

        if ($this->indicator === null) {
            $this->indicator = new ProgressIndicator(
                $this->output,
                'normal',
                100,
                array_map(static fn(string $frame): string => "<fg=cyan>{$frame}</>", self::SPINNER),
                '<fg=green>✔</>',
            );
            $this->indicator->start($message);

            return;
        }

        $this->indicator->setMessage($message);
        $this->indicator->advance();
    }

    public function finish(): void
    {
        if ($this->indicator !== null && $this->last !== null && StageLine::runCompleted($this->last)) {
            $files = StageLine::filesWritten($this->last);
            $this->indicator->finish(sprintf(
                '<options=bold>%s rows</> in <options=bold>%s</> <fg=gray>· peak %s%s</>',
                number_format($this->last->rowsRead),
                StageLine::seconds($this->last->elapsedSeconds),
                StageLine::memory($this->last->peakMemoryBytes),
                $files === null ? '' : ' · ' . number_format($files) . ' files',
            ));
        } elseif ($this->indicator !== null) {
            if ($this->output instanceof ConsoleSectionOutput) {
                $this->output->clear();
            } else {
                (new Cursor($this->output))
                    ->moveToColumn(1)
                    ->clearLine();
            }
        }

        $this->indicator = null;
        $this->last = null;
    }

    private function message(Pulse $pulse): string
    {
        $finished = [];
        $located = [];
        $running = [];

        foreach ($pulse->stages as $stage) {
            $line = new StageLine($stage, $pulse);
            $label = OutputFormatter::escape($line->label());

            if ($stage->endedAtSeconds !== null) {
                $finished[] = '<fg=gray>' . $label . '</> <fg=green>✔</>';

                continue;
            }

            $percent = $line->percent();
            $rate = $line->rate();
            $count =
                $line->shortCount()
                . ($percent === null ? '' : sprintf(' <fg=gray>%d%%</>', $percent))
                . ($rate === null ? '' : ' <fg=yellow>' . $rate . '</>');
            $location = $line->location(self::LOCATION_WIDTH);
            $running[] = '<options=bold>' . $label . '</> ' . $count;
            $located[] =
                '<options=bold>'
                . $label
                . '</> '
                . ($location === null ? '' : '<fg=gray>' . OutputFormatter::escape($location) . '</> ')
                . $count;
        }

        $tail =
            '<fg=gray>'
            . implode(' · ', array_filter([
                $this->output->isVerbose() ? StageLine::memory($pulse->memoryBytes) : null,
                StageLine::seconds($pulse->elapsedSeconds),
            ]))
            . '</>';
        $latest = array_slice($running, -1);
        $candidates = array_map(
            static fn(array $parts): string => implode(' ', array_filter(
                [...$parts, $tail],
                static fn(string $part): bool => $part !== '',
            )),
            [
                [implode(' <fg=gray>›</> ', array_filter([...$finished, implode(' <fg=gray>·</> ', $located)]))],
                [implode(' <fg=gray>›</> ', array_filter([...$finished, implode(' <fg=gray>·</> ', $running)]))],
                [implode(' <fg=gray>·</> ', $running)],
                [implode(' <fg=gray>·</> ', [
                    ...$latest,
                    ...(count($running) > 1 ? ['<fg=gray>+' . (count($running) - 1) . ' running</>'] : []),
                ])],
                [implode('', $latest)],
            ],
        );
        $width = max(1, min(self::MAX_WIDTH, (new Terminal())->getWidth()) - self::INDICATOR_AND_LAST_COLUMN);

        foreach ($candidates as $candidate) {
            if ($this->visible($candidate) <= $width) {
                return $candidate;
            }
        }

        // a line that wraps leaves its first row behind on every redraw
        return !$this->output instanceof ConsoleSectionOutput
            ? OutputFormatter::escape(Helper::substr(
                Helper::removeDecoration($this->output->getFormatter(), $candidates[count($candidates) - 1]),
                0,
                $width,
            ))
            : $candidates[count($candidates) - 1];
    }

    private function visible(string $text): int
    {
        return Helper::width(Helper::removeDecoration($this->output->getFormatter(), $text));
    }
}
