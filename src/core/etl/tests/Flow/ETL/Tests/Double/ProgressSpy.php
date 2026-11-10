<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;

use function array_values;

final readonly class ProgressSpy implements Progress, ProgressStages
{
    /**
     * @param ArrayObject<int, string> $stageLog
     * @param ArrayObject<int, array{string, int}> $advancedLog
     */
    public function __construct(
        private string $stage = '',
        private ArrayObject $stageLog = new ArrayObject(),
        private ArrayObject $advancedLog = new ArrayObject(),
    ) {}

    public function advanced(int $rows): void
    {
        $this->advancedLog->append([$this->stage, $rows]);
    }

    /**
     * @return list<array{string, int}>
     */
    public function advancedCalls(): array
    {
        return array_values($this->advancedLog->getArrayCopy());
    }

    public function ended(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void {}

    public function reported(
        int $rowsRead,
        string $stage,
        int $stageRows,
        int $memoryBytes,
        int $peakMemoryBytes,
    ): void {}

    public function rowsOf(string $stage): int
    {
        $rows = 0;

        foreach ($this->advancedLog as [$name, $advanced]) {
            if ($name === $stage) {
                $rows += $advanced;
            }
        }

        return $rows;
    }

    public function stage(string $name): Progress
    {
        $this->stageLog->append($name);

        return new self($name, $this->stageLog, $this->advancedLog);
    }

    /**
     * @return list<string>
     */
    public function stages(): array
    {
        return array_values($this->stageLog->getArrayCopy());
    }
}
