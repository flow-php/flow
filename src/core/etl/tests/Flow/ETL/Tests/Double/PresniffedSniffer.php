<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;

use function count;

final readonly class PresniffedSniffer implements Sniffer
{
    /**
     * @param list<string> $names
     * @param list<Sample> $samples one per unit, in unit order
     */
    public function __construct(
        private array $names,
        private array $samples,
    ) {}

    public function bytes(int $unit): int
    {
        return 0;
    }

    public function close(): void {}

    public function names(): array
    {
        return $this->names;
    }

    public function sniff(int $unit, ?int $rowBudget): Sample
    {
        return $this->samples[$unit];
    }

    public function parts(int $unit, int $targetBytes): array
    {
        return [$this];
    }

    public function units(): int
    {
        return count($this->samples);
    }
}
