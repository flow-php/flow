<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\Filesystem;
use Throwable;

final readonly class CSVSourceOpener
{
    public function __construct(
        private Filesystem $filesystem,
        private CSVReadOptions $options,
    ) {}

    public function open(SourceFile $source): CSVOpenSource
    {
        $stream = $this->filesystem->readFrom($source->path);

        // the callers' try/finally starts only after open() returns, and detection reads the stream
        try {
            $detected = csv_detect_separator($stream);
            $dialect = new CSVDialect(
                $this->options->separator ?? $detected->separator,
                $this->options->enclosure ?? $detected->enclosure,
                $this->options->escape ?? $detected->escape,
            );

            return new CSVOpenSource(
                $stream,
                $dialect,
                new CSVEncoder(
                    withHeader: $this->options->withHeader,
                    separator: $dialect->separator,
                    enclosure: $dialect->enclosure,
                    escape: $dialect->escape,
                    emptyToNull: $this->options->emptyToNull,
                ),
                new CSVLineReader($dialect->enclosure, $this->options->charactersReadInLine, $this->options->removeBOM),
            );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }
}
