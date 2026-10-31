<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\RustIterator;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\TypeNarrower;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustCSVOpenSource implements CSVOpenSource
{
    /**
     * @param null|list<string> $headers the names of a stream that carries no header record - a window cut past a
     *                                   file's first record; withHeader must be false
     */
    public function __construct(
        SourceStream $stream,
        string $separator,
        string $enclosure,
        string $escape,
        bool $withHeader,
        bool $emptyToNull,
        bool $removeBOM,
        ?int $charactersReadInLine = null,
        ?array $headers = null,
        bool $missingColumnsAsNull = false,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return RustIterator<Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend, ?int $sampleSize = null): RustIterator
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function columns(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function headers(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function producedBytes(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function producedRows(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return RustIterator<array<array-key, ?string>>
     */
    public function records(): RustIterator
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function sniff(array $names, int $rowBudget, TypeNarrower $typer): ColumnTypes
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
