<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;

use function array_keys;
use function array_map;
use function array_values;
use function count;
use function is_int;

final readonly class ProgressSpy implements Progress, ProgressStages
{
    /**
     * @param ArrayObject<int, array{string, int|'busy'|'done'|ProgressUnit}> $advancedLog
     * @param ArrayObject<int, array{string, ProgressUnit, ?int, ?string}> $builtLog
     */
    public function __construct(
        private string $stage = '',
        private ArrayObject $advancedLog = new ArrayObject(),
        private ArrayObject $builtLog = new ArrayObject(),
    ) {}

    public function advanced(int $rows): void
    {
        $this->advancedLog->append([$this->stage, $rows]);
    }

    /**
     * @return list<array{string, int|'busy'|'done'|ProgressUnit}>
     */
    public function advancedCalls(): array
    {
        return array_values($this->advancedLog->getArrayCopy());
    }

    /**
     * @return list<array{string, ProgressUnit, ?int, ?string}>
     */
    public function built(): array
    {
        return array_values($this->builtLog->getArrayCopy());
    }

    /**
     * What $stage counted while it counted in $unit; a stage counts in rows until its first countsIn().
     */
    public function countIn(string $stage, ProgressUnit $unit): int
    {
        $count = 0;
        $current = ProgressUnit::rows;

        foreach ($this->advancedLog as [$name, $logged]) {
            if ($name !== $stage) {
                continue;
            }

            if ($logged instanceof ProgressUnit) {
                $current = $logged;
            } elseif (is_int($logged) && $current === $unit) {
                $count += $logged;
            }
        }

        return $count;
    }

    /**
     * @return list<ProgressUnit> the units $stage switched to, in call order
     */
    public function unitsOf(string $stage): array
    {
        $units = [];

        foreach ($this->advancedLog as [$name, $logged]) {
            if ($name === $stage && $logged instanceof ProgressUnit) {
                $units[] = $logged;
            }
        }

        return $units;
    }

    public function countsIn(ProgressUnit $unit): void
    {
        $this->advancedLog->append([$this->stage, $unit]);
    }

    public function busy(): void
    {
        $this->advancedLog->append([$this->stage, 'busy']);
    }

    public function busyCalls(string $stage): int
    {
        return count(array_keys($this->advancedCalls(), [$stage, 'busy'], true));
    }

    public function done(): void
    {
        $this->advancedLog->append([$this->stage, 'done']);
    }

    public function doneCalls(string $stage): int
    {
        return count(array_keys($this->advancedCalls(), [$stage, 'done'], true));
    }

    public function ended(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes): void {}

    public function reported(
        int $rowsRead,
        array $stages,
        int $memoryBytes,
        int $peakMemoryBytes,
        bool $progressed,
    ): void {}

    public function reportsEverySeconds(): ?float
    {
        return null;
    }

    public function countOf(string $stage): int
    {
        $rows = 0;

        foreach ($this->advancedLog as [$name, $advanced]) {
            if ($name === $stage && is_int($advanced)) {
                $rows += $advanced;
            }
        }

        return $rows;
    }

    public function stage(
        string $name,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $location = null,
    ): Progress {
        $this->builtLog->append([$name, $unit, $id, $location]);

        return new self($name, $this->advancedLog, $this->builtLog);
    }

    /**
     * @return list<string>
     */
    public function stages(): array
    {
        return array_map(static fn(array $built): string => $built[0], $this->built());
    }
}
