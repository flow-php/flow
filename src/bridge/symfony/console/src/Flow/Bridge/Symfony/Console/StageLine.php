<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Symfony\Component\Console\Helper\Helper;

use function array_slice;
use function count;
use function explode;
use function floor;
use function implode;
use function intdiv;
use function number_format;
use function sprintf;
use function str_ends_with;
use function str_replace;
use function strpbrk;
use function strpos;
use function substr;

final readonly class StageLine
{
    private const int SHORTEST_LOCATION = 8;

    private const float SHORTEST_RATE_SECONDS = 0.5;

    public function __construct(
        private StageProgress $stage,
        private Pulse $pulse,
    ) {}

    public static function runCompleted(Pulse $pulse): bool
    {
        foreach ($pulse->stages as $stage) {
            if ($stage->endedAtSeconds === null) {
                return false;
            }
        }

        return $pulse->stages !== [];
    }

    public static function filesWritten(Pulse $pulse): ?int
    {
        $files = null;

        foreach ($pulse->stages as $stage) {
            if ($stage->name === ProgressStages::COMMIT) {
                $files = ($files ?? 0) + $stage->count;
            }
        }

        return $files;
    }

    public static function human(float $value): string
    {
        return match (true) {
            $value >= 1_000_000 => number_format($value / 1_000_000, 1) . 'M',
            $value >= 10_000 => number_format($value / 1_000, 1) . 'K',
            default => number_format($value),
        };
    }

    public static function memory(int $bytes): string
    {
        return number_format($bytes / 1_048_576) . ' MiB';
    }

    public static function seconds(float $seconds): string
    {
        return match (true) {
            $seconds < 60 => number_format($seconds, 1) . 's',
            $seconds < 3600 => intdiv((int) $seconds, 60) . 'm ' . ((int) $seconds % 60) . 's',
            default => intdiv((int) $seconds, 3600) . 'h ' . intdiv((int) $seconds % 3600, 60) . 'm',
        };
    }

    public static function key(StageProgress $stage, int $position): string
    {
        return $stage->id === null ? 'position:' . $position : 'id:' . $stage->id;
    }

    public function duration(): string
    {
        return self::seconds($this->runningSeconds());
    }

    public function exactCount(): string
    {
        return $this->percent() === null
            ? number_format($this->stage->count) . ' ' . $this->stage->unit->name
            : sprintf(
                '%s / %s %s',
                number_format($this->stage->count),
                number_format((int) $this->stage->total),
                $this->stage->unit->name,
            );
    }

    public function label(): string
    {
        return str_replace('_', ' ', $this->stage->name);
    }

    public function location(int $width): ?string
    {
        if ($this->stage->location === null || $width < self::SHORTEST_LOCATION) {
            $location = null;
        } else {
            $separator = strpos($this->stage->location, '://');
            $segments = explode(
                '/',
                $separator === false ? $this->stage->location : substr($this->stage->location, $separator + 3),
            );
            $location = implode('/', array_slice($segments, -match (true) {
                str_ends_with($this->stage->location, '/') => 3,
                strpbrk($segments[count($segments) - 1], '*?[{') !== false => 2,
                default => 1,
            }));
        }

        return $location === null || Helper::length($location) <= $width
            ? $location
            : '…' . Helper::substr($location, -($width - 1));
    }

    public function percent(): ?int
    {
        return $this->stage->total === null || $this->stage->total === 0 || $this->stage->endedAtSeconds !== null
            ? null
            : (int) floor((100 * $this->stage->count) / $this->stage->total);
    }

    /**
     * @return null|float null for groups and files, and for a stage too short for a rate to mean anything
     */
    public function perSecond(): ?float
    {
        $seconds = $this->runningSeconds();

        return $this->stage->unit === ProgressUnit::rows && $seconds >= self::SHORTEST_RATE_SECONDS
            ? $this->stage->count / $seconds
            : null;
    }

    public function rate(): ?string
    {
        $perSecond = $this->perSecond();

        return $perSecond === null ? null : self::human($perSecond) . '/s';
    }

    public function shortCount(): string
    {
        return $this->percent() === null
            ? self::human($this->stage->count) . ' ' . $this->stage->unit->name
            : self::human($this->stage->count) . '/' . self::human((int) $this->stage->total);
    }

    private function runningSeconds(): float
    {
        return ($this->stage->endedAtSeconds ?? $this->pulse->elapsedSeconds) - $this->stage->startedAtSeconds;
    }
}
