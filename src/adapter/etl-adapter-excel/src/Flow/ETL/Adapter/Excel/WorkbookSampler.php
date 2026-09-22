<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Generator;

use function array_key_exists;
use function count;

final class WorkbookSampler implements SchemaSampler
{
    /**
     * @var array<int, WorkbookSheet>
     */
    private array $sheets = [];

    /**
     * @var array<int, WorkbookSheetSample>
     */
    private array $samples = [];

    /**
     * @param list<SourceFile> $files materialised, not a Generator: columns() and samples() both walk it
     */
    public function __construct(
        private readonly WorkbookReader $workbook,
        private readonly array $files,
    ) {}

    public function close(): void
    {
        foreach ($this->sheets as $sheet) {
            $sheet->close();
        }
    }

    public function header(): ExcelHeader
    {
        foreach ($this->files as $index => $file) {
            $names = ($this->sheets[$index] ??= $this->workbook->sheet($file))->columns();

            if ($names !== []) {
                return new ExcelHeader($names, $file->uri());
            }
        }

        return new ExcelHeader([], null);
    }

    /**
     * A bounded sample keeps what it parsed for take(); an unbounded one parses every row, so it streams and closes
     * as it goes. Either way SchemaInferrer stops advancing a sheet once its budget is spent.
     *
     * @return Generator<int, WorkbookSheetSample>
     */
    public function samples(int $rowBudget): iterable
    {
        foreach ($this->files as $index => $file) {
            $sheet = $this->sheets[$index] ??= $this->workbook->sheet($file);

            yield $this->samples[$index] = new WorkbookSheetSample(
                $rowBudget === -1 ? $sheet->rows() : $sheet->sample(),
            );
        }
    }

    /**
     * Exact when the samples read every row of every file, unknown otherwise.
     */
    public function sampledRows(): Cardinality
    {
        $rows = 0;

        foreach ($this->samples as $sample) {
            if (!$sample->wholeSheet()) {
                return Cardinality::unknown();
            }

            $rows += $sample->rows();
        }

        return count($this->samples) === count($this->files) ? Cardinality::exact($rows) : Cardinality::unknown();
    }

    /**
     * The sheet this sampler read $file through, handed over to the caller to read on and close; null when the
     * sample never reached the file.
     */
    public function take(SourceFile $file): ?WorkbookSheet
    {
        foreach ($this->files as $index => $sampled) {
            if ($sampled->uri() === $file->uri() && array_key_exists($index, $this->sheets)) {
                $sheet = $this->sheets[$index];
                unset($this->sheets[$index]);

                return $sheet;
            }
        }

        return null;
    }
}
