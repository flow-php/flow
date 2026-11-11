<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use SensitiveParameter;

final readonly class VoidProgress implements Describable, Progress, ProgressStages
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s is rebuilt by Rebuild::progress()', self::class);
    }

    public function advanced(int $rows): void {}

    public function busy(): void {}

    public function countsIn(ProgressUnit $unit): void {}

    public function describe(): ?Description
    {
        return Description::compose(self::class, []);
    }

    public function done(): void {}

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

    public function stage(
        StageName $stage,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $subject = null,
        ?int $total = null,
    ): Progress {
        return $this;
    }
}
