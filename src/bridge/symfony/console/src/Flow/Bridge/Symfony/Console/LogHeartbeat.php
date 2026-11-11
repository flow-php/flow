<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Output\OutputInterface;

use function intdiv;
use function number_format;
use function sprintf;
use function str_pad;

final class LogHeartbeat implements ConsoleHeartbeat
{
    private const int LABEL_WIDTH = 9;

    private const int SUBJECT_WIDTH = 40;

    private const float SECONDS_BETWEEN_LINES = 10.0;

    private ?Pulse $last = null;

    private float $nextLineAt = self::SECONDS_BETWEEN_LINES;

    /**
     * @var array<string, float> the end that each entry's last done line shows, by key
     */
    private array $printed = [];

    public function __construct(
        private readonly OutputInterface $output,
    ) {}

    private static function stamp(float $seconds): string
    {
        return sprintf('<fg=gray>[%02d:%02d]</>', intdiv((int) $seconds, 60), (int) $seconds % 60);
    }

    public function beat(Pulse $pulse): void
    {
        if ($this->output->isQuiet()) {
            return;
        }

        // a first beat after an earlier one is a new run: its ids and its clock start again
        if ($pulse->beat === 1 && $this->last !== null) {
            $this->nextLineAt = self::SECONDS_BETWEEN_LINES;
            $this->printed = [];
        }

        $this->last = $pulse;
        $progress = $pulse->elapsedSeconds >= $this->nextLineAt;

        if ($progress) {
            $this->nextLineAt = $pulse->elapsedSeconds + self::SECONDS_BETWEEN_LINES;
        }

        foreach ($pulse->stages as $position => $stage) {
            $line = new StageLine($stage, $pulse);
            $perSecond = $line->perSecond();
            $rate = $perSecond === null ? '' : ', ' . StageLine::human($perSecond) . ' rows/s';

            if ($stage->endedAtSeconds === null) {
                if ($progress && $line->started()) {
                    $percent = $line->percent();
                    $this->output->writeln(sprintf(
                        '%s %s%s%s, memory %s',
                        self::stamp($pulse->elapsedSeconds),
                        $this->labelAndSubject($line),
                        $percent === null
                            ? StageLine::counted($stage->count, $stage->unit)
                            : sprintf(
                                '%s of %s (%d%%)',
                                number_format($stage->count),
                                StageLine::counted((int) $stage->total, $stage->unit),
                                $percent,
                            ),
                        $rate,
                        StageLine::memory($pulse->memoryBytes),
                    ));
                }

                continue;
            }

            $key = StageLine::key($stage, $position);

            if (($this->printed[$key] ?? null) === $stage->endedAtSeconds) {
                continue;
            }

            $this->printed[$key] = $stage->endedAtSeconds;

            if (!$line->started()) {
                $this->output->writeln(sprintf(
                    '%s %s<fg=gray>skipped</>',
                    self::stamp($stage->endedAtSeconds),
                    $this->labelAndSubject($line),
                ));

                continue;
            }

            $this->output->writeln(sprintf(
                '%s %s<fg=green>done</> %s in %s%s',
                self::stamp($stage->endedAtSeconds),
                $this->labelAndSubject($line),
                $line->exactCount(),
                $line->duration(),
                $rate,
            ));
        }
    }

    public function finish(): void
    {
        if (!$this->output->isQuiet() && $this->last !== null && StageLine::readCompleted($this->last)) {
            $files = StageLine::filesWritten($this->last);
            $this->output->writeln(sprintf(
                '%s <fg=green>finished</>  %s in %s, peak memory %s%s',
                self::stamp($this->last->elapsedSeconds),
                StageLine::counted($this->last->rowsRead, ProgressUnit::rows),
                StageLine::seconds($this->last->elapsedSeconds),
                StageLine::memory($this->last->peakMemoryBytes),
                $files === null ? '' : ', ' . StageLine::counted($files, ProgressUnit::files) . ' written',
            ));
        }

        $this->last = null;
        $this->nextLineAt = self::SECONDS_BETWEEN_LINES;
        $this->printed = [];
    }

    private function labelAndSubject(StageLine $line): string
    {
        $subject = $line->subject(self::SUBJECT_WIDTH);

        return (
            OutputFormatter::escape(str_pad($line->label(), self::LABEL_WIDTH))
            . ' '
            . ($subject === null ? '' : OutputFormatter::escape($subject) . '  ')
        );
    }
}
