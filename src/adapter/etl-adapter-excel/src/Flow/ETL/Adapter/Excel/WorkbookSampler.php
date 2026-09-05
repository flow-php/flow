<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Generator;

final class WorkbookSampler implements SchemaSampler
{
    /**
     * @var array<int, WorkbookSheet>
     */
    private array $sheets = [];

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
     * $rowBudget is deliberately unused: rows() is lazy and SchemaInferrer stops advancing it.
     *
     * @return Generator<int, Generator<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable
    {
        foreach ($this->files as $index => $file) {
            yield ($this->sheets[$index] ??= $this->workbook->sheet($file))->rows();
        }
    }
}
