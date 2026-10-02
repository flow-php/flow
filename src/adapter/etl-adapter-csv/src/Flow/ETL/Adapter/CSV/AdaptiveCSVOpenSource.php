<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\Filesystem\Filesystem;
use Flow\Types\Type\TypeNarrower;
use Iterator;
use Throwable;

use function extension_loaded;

final readonly class AdaptiveCSVOpenSource implements CSVOpenSource
{
    private CSVOpenSource $source;

    public function __construct(Filesystem $filesystem, SourceFile $source, CSVReadOptions $options)
    {
        $stream = $filesystem->readFrom($source->path);

        // the callers' try/finally starts only after the constructor returns, and detection reads the stream
        try {
            $detected = csv_detect_separator($stream);
            $dialect = new CSVDialect(
                $options->separator ?? $detected->separator,
                $options->enclosure ?? $detected->enclosure,
                $options->escape ?? $detected->escape,
            );

            $this->source = extension_loaded('flow_php')
                ? new RustCSVOpenSource(
                    $stream,
                    $dialect->separator,
                    $dialect->enclosure,
                    $dialect->escape,
                    $options->withHeader,
                    $options->emptyToNull,
                    $options->removeBOM,
                    $options->charactersReadInLine,
                )
                : new PhpCSVOpenSource(
                    $stream,
                    new CSVDecoder(
                        withHeader: $options->withHeader,
                        separator: $dialect->separator,
                        enclosure: $dialect->enclosure,
                        escape: $dialect->escape,
                        emptyToNull: $options->emptyToNull,
                    ),
                    new CSVLineReader(
                        $dialect->enclosure,
                        $dialect->separator,
                        $dialect->escape,
                        $options->charactersReadInLine,
                        $options->removeBOM,
                    ),
                );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }

    public function close(): void
    {
        $this->source->close();
    }

    public function producedBytes(): int
    {
        return $this->source->producedBytes();
    }

    public function producedRows(): int
    {
        return $this->source->producedRows();
    }

    public function columns(): array
    {
        return $this->source->columns();
    }

    public function batches(Schema $schema, int $batchSize, Backend $backend): Iterator
    {
        return $this->source->batches($schema, $batchSize, $backend);
    }

    public function headers(): array
    {
        return $this->source->headers();
    }

    public function records(): Iterator
    {
        return $this->source->records();
    }

    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes
    {
        return $this->source->sniff($names, $rowBudget, $inference, $typer);
    }
}
