<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Filesystem\Stream\RangeSourceStream;
use SensitiveParameter;
use Throwable;

use function count;
use function Flow\Types\DSL\type_array;

use const PHP_INT_MAX;

final readonly class CSVSourceOpener implements Describable
{
    /**
     * The read length the PHP lane's CSVLineReader uses on a local window, where charactersReadInLine (a remote
     * range-request size) would be wasteful; the native lane reads a local file in its own preads and ignores it.
     */
    private const int LOCAL_CHUNK = 1 << 15;

    private bool $native;

    /**
     * @param CSVDialects $dialects the dialects known by uri; a file opened without one records the dialect it detected
     *
     * @throws \Flow\ETL\Foundation\Exception\RuntimeException flow_php is loaded but built for another version of this package
     */
    public function __construct(
        private Filesystem $filesystem,
        private CSVReadOptions $options,
        private CSVDialects $dialects = new CSVDialects(),
        ?FlowPhpExtension $extension = null,
    ) {
        $this->native = ($extension ?? FlowPhpExtension::detect())->available();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            CSVReadOptions::fromArray(type_array()->assert($data['options'] ?? null)),
            CSVDialects::fromArray(type_array()->assert($data['dialects'] ?? null)),
        );
    }

    /**
     * The extension is detected per process, never described.
     */
    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
            'options' => $this->options->normalize(),
            'dialects' => $this->dialects->normalize(),
        ]);
    }

    /**
     * @param null|InferredColumns $inferred the columns the file must carry; checked before a row is built
     *
     * @throws InferredSchemaException
     */
    public function open(SourceFile $source, ?InferredColumns $inferred = null): CSVOpenSource
    {
        $stream = $this->filesystem->readFrom($source->path);

        // the callers' try/finally starts only after open() returns, and detection reads the stream
        try {
            $dialect = $this->dialects->for($source->uri()) ?? $this->detected($stream, $source);
            $inferred?->refuseDivergence($source->uri(), $this->columns($stream, $dialect));

            return $this->over($stream, $dialect, null, $this->options->charactersReadInLine);
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }

    /**
     * The records whose first byte lies in the split's window. Both window ends are resynced to a record start, so a
     * split ends exactly where its neighbour begins; only the first split reads the header and the BOM, and only the
     * first split refuses a header diverging from $inferred - once per file.
     *
     * @throws InferredSchemaException
     */
    public function openSplit(SourceFile $source, FileSplit $split, ?InferredColumns $inferred = null): CSVOpenSource
    {
        $stream = $this->filesystem->readFrom($source->path);

        try {
            $dialect = $this->dialects->for($source->uri()) ?? $this->detected($stream, $source);
            $columns = $this->columns($stream, $dialect);

            if ($split->isFirst()) {
                $inferred?->refuseDivergence($source->uri(), $columns);
            }

            $resync = $columns === [] ? null : new CSVRecordResync($dialect, count($columns));

            return $this->over(
                $resync === null
                    ? new RangeSourceStream($stream, 0, 0)
                    : $split->window(
                        $stream,
                        $resync->after($stream, $split->start),
                        $split->length === null ? null : $resync->after($stream, $split->start + $split->length),
                    ),
                $dialect,
                $split->isFirst() ? null : $columns,
                $stream instanceof NativeLocalSourceStream ? self::LOCAL_CHUNK : $this->options->charactersReadInLine,
            );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }

    /**
     * The column names of $stream's first record - the header, or the e00.. names; $stream stays open: the view it is
     * read through is never closed, closing it would close $stream.
     *
     * @return list<string>
     */
    public function columns(SourceStream $stream, CSVDialect $dialect): array
    {
        return $this->over(
            new RangeSourceStream($stream, 0, $stream->size() ?? PHP_INT_MAX),
            $dialect,
            null,
            $this->options->charactersReadInLine,
        )->columns();
    }

    /**
     * A fully declared dialect reads nothing; otherwise the undeclared parts are detected from $stream.
     */
    public function dialect(SourceStream $stream): CSVDialect
    {
        if (
            $this->options->separator !== null
            && $this->options->enclosure !== null
            && $this->options->escape !== null
        ) {
            return new CSVDialect($this->options->separator, $this->options->enclosure, $this->options->escape);
        }

        $detected = (new CSVDetector($stream))->detect();

        return new CSVDialect(
            $this->options->separator ?? $detected->separator,
            $this->options->enclosure ?? $detected->enclosure,
            $this->options->escape ?? $detected->escape,
        );
    }

    private function detected(SourceStream $stream, SourceFile $source): CSVDialect
    {
        $dialect = $this->dialect($stream);
        $this->dialects->learn($source->uri(), $dialect);

        return $dialect;
    }

    /**
     * @param null|list<string> $headers null reads $stream from a file's start - header and BOM as the options say;
     *                                   a list names the records of a window past the first one
     * @param null|int<1, max> $charactersReadInLine
     */
    public function over(
        SourceStream $stream,
        CSVDialect $dialect,
        ?array $headers,
        ?int $charactersReadInLine,
    ): CSVOpenSource {
        $withHeader = $headers === null && $this->options->withHeader;
        $removeBOM = $headers === null && $this->options->removeBOM;

        if ($this->native) {
            return new RustCSVOpenSource(
                $stream,
                $dialect->separator,
                $dialect->enclosure,
                $dialect->escape,
                $withHeader,
                $this->options->emptyToNull,
                $removeBOM,
                $charactersReadInLine,
                $headers,
            );
        }

        return new PhpCSVOpenSource(
            $stream,
            new CSVDecoder(
                withHeader: $withHeader,
                separator: $dialect->separator,
                enclosure: $dialect->enclosure,
                escape: $dialect->escape,
                emptyToNull: $this->options->emptyToNull,
                headers: $headers,
            ),
            new CSVLineReader(
                $dialect->enclosure,
                $dialect->separator,
                $dialect->escape,
                $charactersReadInLine,
                $removeBOM,
            ),
        );
    }
}
