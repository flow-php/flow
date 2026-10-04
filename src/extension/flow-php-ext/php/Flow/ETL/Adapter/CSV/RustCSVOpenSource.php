<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\RustIterator;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\TypeNarrower;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustCSVOpenSource implements CSVOpenSource
{
    public function __construct(
        SourceStream $stream,
        string $separator,
        string $enclosure,
        string $escape,
        bool $withHeader,
        bool $emptyToNull,
        bool $removeBOM,
        ?int $charactersReadInLine = null,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return RustIterator<Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): RustIterator
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

    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
