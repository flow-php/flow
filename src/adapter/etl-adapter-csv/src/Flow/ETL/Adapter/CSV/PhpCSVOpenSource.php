<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\TypeNarrower;
use Generator;

final readonly class PhpCSVOpenSource implements CSVOpenSource
{
    public function __construct(
        private SourceStream $stream,
        private CSVEncoder $encoder,
        private CSVLineReader $lineReader,
    ) {}

    public function close(): void
    {
        $this->stream->close();
    }

    /**
     * This instance is consumed afterwards.
     *
     * @return list<string>
     */
    public function columns(): array
    {
        foreach ($this->lineReader->readLines($this->stream) as $line) {
            $this->encoder->decode([$line]);

            break;
        }

        return $this->encoder->headers() ?? [];
    }

    /**
     * CSVLineReader::readLines() already joins a quoted multi-line record, so never re-split or re-join here.
     * This instance is consumed afterwards.
     *
     * @return Generator<int, RawRowValues>
     */
    public function records(): Generator
    {
        foreach ($this->lineReader->readLines($this->stream) as $line) {
            foreach ($this->encoder->decode([$line]) as $values) {
                yield $values;
            }
        }
    }

    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes
    {
        return (new SchemaInferrer($inference, $typer))->sniff($names, $this->records(), $rowBudget);
    }
}
