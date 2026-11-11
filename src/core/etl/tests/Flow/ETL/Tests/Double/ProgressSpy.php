<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;

use function array_keys;
use function array_map;
use function array_values;
use function count;
use function is_int;

final readonly class ProgressSpy implements Progress, ProgressStages
{
    private string $stage;

    /**
     * @param ArrayObject<int, array{string, int|'busy'|'done'|ProgressUnit}> $advancedLog
     * @param ArrayObject<int, array{string, ProgressUnit, ?int, ?string}> $builtLog
     */
    public function __construct(
        ?StageName $stage = null,
        private ArrayObject $advancedLog = new ArrayObject(),
        private ArrayObject $builtLog = new ArrayObject(),
    ) {
        $this->stage = $stage->value ?? '';
    }

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
    public function countIn(StageName $stage, ProgressUnit $unit): int
    {
        $count = 0;
        $current = ProgressUnit::rows;

        foreach ($this->advancedLog as [$name, $logged]) {
            if ($name !== $stage->value) {
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
    public function unitsOf(StageName $stage): array
    {
        $units = [];

        foreach ($this->advancedLog as [$name, $logged]) {
            if ($name === $stage->value && $logged instanceof ProgressUnit) {
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

    public function busyCalls(StageName $stage): int
    {
        return count(array_keys($this->advancedCalls(), [$stage->value, 'busy'], true));
    }

    public function done(): void
    {
        $this->advancedLog->append([$this->stage, 'done']);
    }

    public function doneCalls(StageName $stage): int
    {
        return count(array_keys($this->advancedCalls(), [$stage->value, 'done'], true));
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

    /**
     * @param null|StageName $stage null: what a spy built with no stage counted
     */
    public function countOf(?StageName $stage): int
    {
        $rows = 0;

        foreach ($this->advancedLog as [$name, $advanced]) {
            if ($name === ($stage->value ?? '') && is_int($advanced)) {
                $rows += $advanced;
            }
        }

        return $rows;
    }

    public function stage(
        StageName $stage,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $subject = null,
        ?int $total = null,
    ): Progress {
        $this->builtLog->append([$stage->value, $unit, $id, $subject]);

        return new self($stage, $this->advancedLog, $this->builtLog);
    }

    /**
     * @return list<string>
     */
    public function stages(): array
    {
        return array_map(static fn(array $built): string => $built[0], $this->built());
    }
}
