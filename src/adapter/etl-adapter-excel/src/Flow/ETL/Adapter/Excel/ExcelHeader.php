<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

final readonly class ExcelHeader
{
    /**
     * $source is a plain label, never a Path or a SourceFile. It is null exactly when $names is empty - no listed
     * workbook resolved a header.
     *
     * @param list<string> $names
     */
    public function __construct(
        public array $names,
        public ?string $source,
    ) {}
}
