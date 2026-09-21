<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Types\Type;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Type\TypeNarrower;
use Generator;

use function array_filter;
use function array_map;
use function array_values;
use function class_exists;
use function extension_loaded;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function max;

final readonly class NativeCSVOpenSource implements CSVOpenSource
{
    /**
     * Each chunk is held as a PHP string, so it sets the path's peak memory. 32 KB measured 12.0 MB real peak - the PHP
     * path's own - on a 57 MB / 11-column file (~600 B/row) and a 54 MB / 5-column file (~4 KB/row); 64 KB already
     * reached 14.0 MB on the wide one. The headroom depends on the file shape.
     */
    public const int CHUNK = 1 << 15;

    private const int BATCH = 1000;

    /**
     * @var int<1, max>
     */
    private int $chunkSize;

    /**
     * @param null|int<1, max> $charactersReadInLine
     */
    public function __construct(
        private SourceStream $stream,
        private RustCSVReaderNative $reader,
        ?int $charactersReadInLine = null,
    ) {
        // a local file ignores the read length, as NativeLocalSourceStream::readLines() already does on the PHP path;
        // for S3/Azure it is the range-request size - and from_csv() defaults it to 10 MB, which would be the chunk
        $this->chunkSize = $stream instanceof NativeLocalSourceStream
            ? self::CHUNK
            : $charactersReadInLine ?? self::CHUNK;
    }

    public static function isSupported(): bool
    {
        return extension_loaded('flow_php') && class_exists(RustCSVReaderNative::class, false);
    }

    public function close(): void
    {
        $this->stream->close();
    }

    public function columns(): array
    {
        foreach ($this->stream->iterate($this->chunkSize) as $chunk) {
            $this->reader->feed($chunk);

            $headers = $this->reader->headers();

            if ($headers !== []) {
                return $headers;
            }
        }

        $this->reader->finish();

        return $this->reader->headers();
    }

    public function records(): Generator
    {
        foreach ($this->stream->iterate($this->chunkSize) as $chunk) {
            $this->reader->feed($chunk);

            while (($batch = $this->reader->next(self::BATCH)) !== []) {
                foreach ($batch as $values) {
                    yield $values;
                }
            }
        }

        $this->reader->finish();

        while (($batch = $this->reader->next(self::BATCH)) !== []) {
            foreach ($batch as $values) {
                yield $values;
            }
        }
    }

    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes
    {
        // the Rust fold is StringTypeNarrower without its HTML and XML rungs - a DOMDocument per cell is not worth porting
        if (!$typer instanceof StringTypeNarrower || $typer->emitsType(type_html()) || $typer->emitsType(type_xml())) {
            return (new SchemaInferrer($inference, $typer))->sniff($names, $this->records(), $rowBudget);
        }

        $fold = new RustColumnFoldNative($names, array_map(
            static fn(Type $type): string => $type->toString(),
            array_values(array_filter(
                [
                    type_json(),
                    type_uuid(),
                    type_float(),
                    type_integer(),
                    type_datetime(),
                    type_date(),
                    type_boolean(),
                    type_time_zone(),
                ],
                $typer->emitsType(...),
            )),
        ));

        foreach ($this->stream->iterate($this->chunkSize) as $chunk) {
            $this->reader->feed($chunk);
            $this->reader->fold($fold, $rowBudget === -1 ? -1 : max(0, $rowBudget - $fold->rows()));

            if ($rowBudget !== -1 && $fold->rows() >= $rowBudget) {
                break;
            }
        }

        if ($rowBudget === -1 || $fold->rows() < $rowBudget) {
            $this->reader->finish();
            $this->reader->fold($fold, $rowBudget === -1 ? -1 : max(0, $rowBudget - $fold->rows()));
        }

        return ColumnTypes::fromColumnTypes(
            array_map(TypeFactory::fromString(...), $fold->types()),
            $fold->rows(),
            $typer,
        );
    }
}
