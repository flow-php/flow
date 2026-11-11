<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Runtime\Heartbeat\Pulse;

use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function floor;
use function implode;
use function max;
use function min;
use function sprintf;
use function str_repeat;

final class ThroughputGraph
{
    private const array BARS = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    private const int POINTS = 12;

    /**
     * @var array<string, array{int, float, ProgressUnit}> the count, the second and the unit of the previous beat
     */
    private array $previous = [];

    /**
     * @var array<string, list<float>> the rate between every two beats, by entry
     */
    private array $rates = [];

    public function recorded(Pulse $pulse): void
    {
        // the first beat of a run: ids start again
        if ($pulse->beat === 1) {
            $this->previous = [];
            $this->rates = [];
        }

        foreach ($pulse->stages as $position => $stage) {
            if ($stage->endedAtSeconds !== null) {
                continue;
            }

            $key = StageLine::key($stage, $position);

            // a count that starts again in another unit is a new history, not a drop
            if (($this->previous[$key][2] ?? $stage->unit) !== $stage->unit) {
                unset($this->previous[$key], $this->rates[$key]);
            }

            [$countBefore, $secondBefore] = $this->previous[$key] ?? [0, $stage->startedAtSeconds];
            $this->rates[$key][] = $pulse->elapsedSeconds > $secondBefore
                ? ($stage->count - $countBefore) / ($pulse->elapsedSeconds - $secondBefore)
                : 0.0;
            $this->previous[$key] = [$stage->count, $pulse->elapsedSeconds, $stage->unit];
        }
    }

    public function sparkline(StageProgress $stage, int $position): string
    {
        $key = StageLine::key($stage, $position);

        if (!array_key_exists($key, $this->rates)) {
            return '';
        }

        $recent = array_slice($this->rates[$key], -self::POINTS);
        $top = max(1.0, max($recent));

        return sprintf(
            '<fg=%s>%s%s</>',
            $stage->endedAtSeconds === null ? 'cyan' : 'gray',
            str_repeat(' ', max(0, self::POINTS - count($recent))),
            implode('', array_map(
                static fn(float $rate): string => self::BARS[(int) max(0, min(7, floor(($rate / $top) * 7)))],
                $recent,
            )),
        );
    }
}
