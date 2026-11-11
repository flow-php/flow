<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Foundation\VoidProgress;
use SensitiveParameter;

final class HeartbeatStage implements Describable, Progress
{
    private readonly string $key;

    private bool $open = false;

    private bool $opened = false;

    private int $reportedMemoryBytes = 0;

    private int $reportedPeakMemoryBytes = 0;

    private int $reportedRowsRead = 0;

    /**
     * @var list<StageProgress>
     */
    private array $reportedStages = [];

    public function __construct(
        private readonly StageName $name,
        private readonly ProgressUnit $unit,
        private readonly ?int $id,
        private readonly ?string $subject,
        private readonly HeartbeatThrottle $throttle,
        private readonly int $object,
        private readonly ?int $total = null,
    ) {
        $this->key = $id === null ? 'object:' . $object : 'id:' . $id;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s is rebuilt by Rebuild::progress()', self::class);
    }

    public function advanced(int $rows): void
    {
        $this->open();
        $this->throttle->stageAdvanced($this->key, $rows);
    }

    public function busy(): void
    {
        $this->open();
        $this->throttle->busy();
    }

    public function countsIn(ProgressUnit $unit): void
    {
        $this->open();
        $this->throttle->stageCountsIn($this->key, $unit);
    }

    public function describe(): ?Description
    {
        return (
            $this->id === null
                ? Description::compose(VoidProgress::class, [])
                : Description::compose(self::class, [
                    'name' => $this->name->value,
                    'id' => $this->id,
                    'subject' => $this->subject,
                ])
        );
    }

    public function done(): void
    {
        // a stage that never opened still closes: one made before planned() is then never listed
        if ($this->open || !$this->opened) {
            $this->open = false;
            $this->throttle->stageClosed($this->key);
        }
    }

    /**
     * @param list<StageProgress> $stages
     */
    public function ended(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes): void
    {
        $this->fold($rowsRead, $stages, $memoryBytes, $peakMemoryBytes, false);
    }

    /**
     * Rows read from the source; no beat, the reader of the source beats.
     */
    public function read(int $rows): void
    {
        $this->open();
        $this->throttle->stageRead($this->key, $rows);
    }

    /**
     * @param list<StageProgress> $stages
     */
    public function reported(
        int $rowsRead,
        array $stages,
        int $memoryBytes,
        int $peakMemoryBytes,
        bool $progressed,
    ): void {
        $this->fold($rowsRead, $stages, $memoryBytes, $peakMemoryBytes, $progressed);
        $this->throttle->beatWhenDue();
    }

    public function reportsEverySeconds(): float
    {
        return $this->throttle->everySeconds();
    }

    /**
     * @param list<StageProgress> $stages
     */
    private function fold(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes, bool $progressed): void
    {
        if (
            !$progressed
            && $rowsRead === $this->reportedRowsRead
            && $stages == $this->reportedStages
            && $memoryBytes === $this->reportedMemoryBytes
            && $peakMemoryBytes === $this->reportedPeakMemoryBytes
        ) {
            return;
        }

        $addedRows = $rowsRead - $this->reportedRowsRead;
        $addedMemoryBytes = $memoryBytes - $this->reportedMemoryBytes;
        $this->reportedRowsRead = $rowsRead;
        $this->reportedStages = $stages;
        $this->reportedMemoryBytes = $memoryBytes;
        $this->reportedPeakMemoryBytes = $peakMemoryBytes;

        $this->throttle->othersReported(
            $this->object,
            $addedRows,
            $stages,
            $addedMemoryBytes,
            $peakMemoryBytes,
            $progressed,
        );
    }

    private function open(): void
    {
        if (!$this->open) {
            $this->open = true;
            $this->opened = true;
            $this->throttle->stageOpened($this->key, $this->id, $this->name, $this->unit, $this->subject, $this->total);
        }
    }
}
