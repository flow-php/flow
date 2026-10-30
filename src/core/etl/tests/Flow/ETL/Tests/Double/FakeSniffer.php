<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\Types\Type\TypeNarrower;

use function count;

final class FakeSniffer implements Sniffer
{
    /**
     * @var list<array{int, int}> unit, row budget - in call order
     */
    public array $sniffed = [];

    public readonly RecordingSources $sources;

    /**
     * @param list<list<array<array-key, mixed>>> $units
     * @param list<string> $names
     */
    public function __construct(
        private readonly array $units,
        private readonly TypeNarrower $typer,
        private readonly array $names = [],
    ) {
        $this->sources = new RecordingSources($units);
    }

    public function bytes(int $unit): int
    {
        return 0;
    }

    public function close(): void {}

    public function names(): array
    {
        return $this->names;
    }

    public function sniff(int $unit, int $rowBudget): Sample
    {
        $this->sniffed[] = [$unit, $rowBudget];
        $columnTypes = ColumnTypes::sniff($this->names, $this->sources->source($unit), $rowBudget, $this->typer);

        return new Sample($columnTypes->folded(), 0, $rowBudget === -1 || $columnTypes->rows() < $rowBudget);
    }

    public function parts(int $unit, int $targetBytes): array
    {
        return [$this];
    }

    public function units(): int
    {
        return count($this->units);
    }
}
