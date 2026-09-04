<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

final readonly class CSVHeader
{
    /**
     * $source is a plain label, never a Path or a SourceFile, so a sampler with no file can supply its own.
     * It is null exactly when $names is empty - no listed source resolved a header.
     *
     * @param list<string> $names
     */
    public function __construct(
        public array $names,
        public ?string $source,
    ) {}
}
