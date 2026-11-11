<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

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

    private const int LOCATION_WIDTH = 40;

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
                if ($progress) {
                    $percent = $line->percent();
                    $this->output->writeln(sprintf(
                        '%s %s%s%s, memory %s',
                        self::stamp($pulse->elapsedSeconds),
                        $this->labelAndLocation($line),
                        $percent === null
                            ? number_format($stage->count) . ' ' . $stage->unit->name
                            : sprintf(
                                '%s of %s %s (%d%%)',
                                number_format($stage->count),
                                number_format((int) $stage->total),
                                $stage->unit->name,
                                $percent,
                            ),
                        $rate,
                        StageLine::memory($pulse->memoryBytes),
                    ));
                }

                continue;
            }

            $key = StageLine::key($stage, $position);

            if (($this->printed[$key] ?? null) !== $stage->endedAtSeconds) {
                $this->printed[$key] = $stage->endedAtSeconds;
                $this->output->writeln(sprintf(
                    '%s %s<fg=green>done</> %s in %s%s',
                    self::stamp($stage->endedAtSeconds),
                    $this->labelAndLocation($line),
                    $line->exactCount(),
                    $line->duration(),
                    $rate,
                ));
            }
        }
    }

    public function finish(): void
    {
        if (!$this->output->isQuiet() && $this->last !== null && StageLine::runCompleted($this->last)) {
            $files = StageLine::filesWritten($this->last);
            $this->output->writeln(sprintf(
                '%s <fg=green>finished</>  %s rows in %s, peak memory %s%s',
                self::stamp($this->last->elapsedSeconds),
                number_format($this->last->rowsRead),
                StageLine::seconds($this->last->elapsedSeconds),
                StageLine::memory($this->last->peakMemoryBytes),
                $files === null ? '' : ', ' . number_format($files) . ' files written',
            ));
        }

        $this->last = null;
        $this->nextLineAt = self::SECONDS_BETWEEN_LINES;
        $this->printed = [];
    }

    private function labelAndLocation(StageLine $line): string
    {
        $location = $line->location(self::LOCATION_WIDTH);

        return (
            OutputFormatter::escape(str_pad($line->label(), self::LABEL_WIDTH))
            . ' '
            . ($location === null ? '' : OutputFormatter::escape($location) . '  ')
        );
    }
}
