<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
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
use function str_ends_with;
use function str_replace;
use function strpbrk;
use function strpos;
use function substr;

final readonly class StageLine
{
    private const int SHORTEST_SUBJECT = 8;

    private const float SHORTEST_RATE_SECONDS = 0.5;

    public function __construct(
        private StageProgress $stage,
        private Pulse $pulse,
    ) {}

    public static function readCompleted(Pulse $pulse): bool
    {
        $read = false;

        foreach ($pulse->stages as $stage) {
            if ($stage->endedAtSeconds === null) {
                return false;
            }

            $read = $read || $stage->name === StageName::read;
        }

        return $read;
    }

    public static function filesWritten(Pulse $pulse): ?int
    {
        $files = null;

        foreach ($pulse->stages as $stage) {
            if ($stage->name === StageName::commit) {
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

    public static function counted(int $count, ProgressUnit $unit): string
    {
        return number_format($count) . ' ' . self::unitName($count, $unit);
    }

    public static function key(StageProgress $stage, int $position): string
    {
        return $stage->id === null ? 'position:' . $position : 'id:' . $stage->id;
    }

    public function duration(): ?string
    {
        return $this->started() ? self::seconds($this->runningSeconds()) : null;
    }

    public function exactCount(): string
    {
        return $this->percent() === null
            ? self::counted($this->stage->count, $this->stage->unit)
            : number_format($this->stage->count) . ' / ' . self::counted((int) $this->stage->total, $this->stage->unit);
    }

    public function started(): bool
    {
        return $this->stage->startedAtSeconds !== null;
    }

    public function label(): string
    {
        return str_replace('_', ' ', $this->stage->name->value);
    }

    public function subject(int $width): ?string
    {
        if ($this->stage->subject === null || $width < self::SHORTEST_SUBJECT) {
            $subject = null;
        } else {
            $separator = strpos($this->stage->subject, '://');
            $segments = explode(
                '/',
                $separator === false ? $this->stage->subject : substr($this->stage->subject, $separator + 3),
            );
            $subject = implode('/', array_slice($segments, -match (true) {
                str_ends_with($this->stage->subject, '/') => 3,
                strpbrk($segments[count($segments) - 1], '*?[{') !== false => 2,
                default => 1,
            }));
        }

        return $subject === null || Helper::length($subject) <= $width
            ? $subject
            : '…' . Helper::substr($subject, -($width - 1));
    }

    public function percent(): ?int
    {
        return $this->stage->total === null
        || $this->stage->total === 0
        || $this->stage->endedAtSeconds !== null
        || !$this->started()
            ? null
            : (int) floor((100 * $this->stage->count) / $this->stage->total);
    }

    /**
     * @return null|float null for groups and files, a stage not started, and one too short for a rate to mean anything
     */
    public function perSecond(): ?float
    {
        $seconds = $this->runningSeconds();

        return $this->stage->unit === ProgressUnit::rows && $this->started() && $seconds >= self::SHORTEST_RATE_SECONDS
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
            ? self::human($this->stage->count) . ' ' . self::unitName($this->stage->count, $this->stage->unit)
            : self::human($this->stage->count) . '/' . self::human((int) $this->stage->total);
    }

    public static function unitName(int $count, ProgressUnit $unit): string
    {
        if ($count !== 1) {
            return $unit->name;
        }

        return match ($unit) {
            ProgressUnit::rows => 'row',
            ProgressUnit::groups => 'group',
            ProgressUnit::files => 'file',
        };
    }

    private function runningSeconds(): float
    {
        return ($this->stage->endedAtSeconds ?? $this->pulse->elapsedSeconds) - (float) $this->stage->startedAtSeconds;
    }
}
