<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\IO\Inference;

use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetReader;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\Types\Type\TypeNarrower;
use Generator;

final class GoogleSheetSniffer implements Sniffer
{
    private ?GoogleSheetSample $sample = null;

    /**
     * @param int<1, max>|-1 $rowBudget
     */
    public function __construct(
        private readonly GoogleSheetReader $reader,
        private readonly int $rowBudget,
        private readonly TypeNarrower $typer,
    ) {}

    /**
     * The raw rows the sample already holds, keyed by their number in the sheet, when its range covered the whole
     * grid - so a read does not fetch the same rows a second time. Null when it did not, and the caller must read the
     * sheet itself.
     *
     * @return null|Generator<int, list<mixed>>
     */
    public function rows(): ?Generator
    {
        if (!$this->sample()->wholeSheet) {
            return null;
        }

        return (function (): Generator {
            yield from $this->sample()->cells;
        })();
    }

    public function bytes(int $unit): int
    {
        return 0;
    }

    public function close(): void {}

    public function gridRowCount(): int
    {
        return $this->sample()->gridRowCount;
    }

    /**
     * @return list<string>
     */
    public function header(): array
    {
        return $this->sample()->names;
    }

    public function sample(): GoogleSheetSample
    {
        // one range is one HTTP round trip, and header() and sniff() both need it
        return $this->sample ??= $this->reader->sample($this->rowBudget);
    }

    public function names(): array
    {
        return $this->header();
    }

    /**
     * The budget sized the single range when this sniffer was constructed, and sample() is memoised, so a sniff
     * folds rows already fetched.
     */
    public function sniff(int $unit, int $rowBudget): Sample
    {
        $columnTypes = ColumnTypes::sniff($this->names(), $this->sample()->rows, $rowBudget, $this->typer);

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
}
