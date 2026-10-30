<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\Memory;

use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\ETL\Values\Column\ColumnName;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\TypeNarrower;
use Generator;

final readonly class InMemoryRows implements Sniffer
{
    /**
     * @param iterable<array<mixed>> $rows
     */
    public function __construct(
        private iterable $rows,
        private TypeNarrower $typer = new InstanceOfTypeNarrower(),
    ) {}

    public function bytes(int $unit): int
    {
        return 0;
    }

    public function close(): void {}

    public function names(): array
    {
        return [];
    }

    public function sniff(int $unit, int $rowBudget): Sample
    {
        $columnTypes = ColumnTypes::sniff([], $this->values(), $rowBudget, $this->typer);

        return new Sample($columnTypes->folded(), 0, $rowBudget === -1 || $columnTypes->rows() < $rowBudget);
    }

    public function parts(int $unit, int $targetBytes): array
    {
        return [$this];
    }

    public function units(): int
    {
        return 1;
    }

    /**
     * The same ColumnName normalisation array_to_rows() applies, so the fold's names are the
     * batch builder's names.
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public function values(): Generator
    {
        $columnName = new ColumnName();

        foreach ($this->rows as $row) {
            $values = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $key => $value) {
                $values[$columnName->of($key)] = $value;
            }

            yield $values;
        }
    }
}
