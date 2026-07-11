<?php

declare(strict_types=1);

namespace Flow\Floe;

use Closure;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Generator;
use Throwable;

use function array_values;
use function count;
use function extension_loaded;
use function json_decode;
use function max;
use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

use const JSON_THROW_ON_ERROR;

final class FloeFile
{
    private ?Footer $footer = null;

    private readonly RowHydrator $rowHydrator;

    private readonly SchemaDecoder $schemaDecoder;

    private readonly bool $useExtension;

    /**
     * @param Closure(): SourceStream $openSource opens a fresh source stream on each read
     * @param null|bool $useExtension null auto-detects the flow_php extension; the explicit flag exists for parity tests
     */
    public function __construct(
        private readonly Closure $openSource,
        private readonly string $uri,
        private readonly Codec $codec,
        private readonly int $chunkSize,
        ?bool $useExtension,
    ) {
        $this->schemaDecoder = new SchemaDecoder(new ValueDecoder(), new EntryInstantiator());
        $this->rowHydrator = new RowHydrator();
        $this->useExtension = $useExtension ?? extension_loaded('flow_php');
    }

    /**
     * @throws FloeException
     */
    public function footer(): Footer
    {
        if ($this->footer !== null) {
            return $this->footer;
        }

        $source = ($this->openSource)();
        $size = $source->size();

        if ($size === null) {
            throw new FloeException(sprintf(
                'Floe footer requires a sized stream, "%s" does not report its size',
                $this->uri,
            ));
        }

        if ($size < (Format::HEADER_LENGTH + Format::TRAILER_LENGTH)) {
            throw new FloeException(sprintf(
                'Floe file "%s" is torn, too small to hold a header and a trailer',
                $this->uri,
            ));
        }

        $flags = Format::validateHeader($source->read(Format::HEADER_LENGTH, 0));

        if ($flags !== $this->codec->id()) {
            throw new FloeException(sprintf(
                'Floe file "%s" was written with codec 0x%02X, expected 0x%02X',
                $this->uri,
                $flags,
                $this->codec->id(),
            ));
        }

        $footerLength = Format::parseTrailer($source->read(Format::TRAILER_LENGTH, $size - Format::TRAILER_LENGTH));

        if (($size - Format::TRAILER_LENGTH - $footerLength) < Format::HEADER_LENGTH) {
            throw new FloeException(sprintf('Floe file "%s" is torn, footer does not fit inside the file', $this->uri));
        }

        /** @var int<1, max> $footerLength */
        $footer = Footer::fromJson($source->read($footerLength, $size - Format::TRAILER_LENGTH - $footerLength));
        $source->close();

        return $this->footer = $footer;
    }

    /**
     * @throws FloeException
     */
    public function metadata(): Metadata
    {
        return $this->footer()->metadata;
    }

    /**
     * Salvages complete frames from the beginning of the file sequentially - no
     * footer, no ranged reads. Rows are yielded as they were written (no
     * padding); reading ends silently at the first truncated or invalid frame.
     *
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    public function recover(int $batchSize = 1000): Generator
    {
        $frames = (new FrameReader(($this->openSource)(), $this->codec->id(), $this->chunkSize))->frames(lenient: true);

        $extDecoder = $this->useExtension ? new RowsDecoder() : null;
        $plan = null;
        $partitions = [];
        $batch = [];
        $pending = [];

        foreach ($frames as [$frameType, $frameBody]) {
            if ($frameType === Format::FRAME_ROW) {
                if ($extDecoder !== null) {
                    try {
                        $pending[] = $this->codec->decode($frameBody);
                    } catch (Throwable) {
                        break;
                    }

                    if (count($pending) === $batchSize) {
                        [$ready, $stop] = $this->flushRecoverPending(
                            $extDecoder,
                            $pending,
                            $batch,
                            $batchSize,
                            $partitions,
                        );
                        $pending = [];

                        foreach ($ready as $rows) {
                            yield $rows;
                        }

                        if ($stop) {
                            break;
                        }
                    }

                    continue;
                }

                try {
                    $row = $this->hydrate(null, $plan, $this->codec->decode($frameBody));
                } catch (Throwable) {
                    break;
                }

                $batch[] = $row;

                if (count($batch) === $batchSize) {
                    yield $this->batch($batch, $partitions);
                    $batch = [];
                }
            } elseif ($frameType === Format::FRAME_SCHEMA) {
                if ($extDecoder !== null && $pending !== []) {
                    [$ready, $stop] = $this->flushRecoverPending(
                        $extDecoder,
                        $pending,
                        $batch,
                        $batchSize,
                        $partitions,
                    );
                    $pending = [];

                    foreach ($ready as $rows) {
                        yield $rows;
                    }

                    if ($stop) {
                        break;
                    }
                }

                try {
                    if ($extDecoder !== null) {
                        $extDecoder->schema($frameBody);
                    } else {
                        $plan = $this->schemaDecoder->decode($frameBody);
                    }
                } catch (Throwable) {
                    break;
                }
            } elseif ($frameType === Format::FRAME_PARTITIONS) {
                if ($extDecoder !== null && $pending !== []) {
                    [$ready, $stop] = $this->flushRecoverPending(
                        $extDecoder,
                        $pending,
                        $batch,
                        $batchSize,
                        $partitions,
                    );
                    $pending = [];

                    foreach ($ready as $rows) {
                        yield $rows;
                    }

                    if ($stop) {
                        break;
                    }
                }

                $partitions = self::decodePartitions($frameBody);
            } elseif ($frameType !== Format::FRAME_FOOTER) {
                break;
            }
        }

        if ($extDecoder !== null && $pending !== []) {
            [$ready] = $this->flushRecoverPending($extDecoder, $pending, $batch, $batchSize, $partitions);

            foreach ($ready as $rows) {
                yield $rows;
            }
        }

        if ($batch !== []) {
            yield $this->batch($batch, $partitions);
        }
    }

    /**
     * A failed batch is replayed row by row so the prefix before the first
     * corrupt body still surfaces; the true stop flag ends the recover read.
     *
     * @param array<int, string> $pending
     * @param array<int, Row> $batch
     * @param int<1, max> $batchSize
     * @param array<int, Partition> $partitions
     *
     * @return array{0: array<int, Rows>, 1: bool} ready batches + stop flag
     */
    private function flushRecoverPending(
        RowsDecoder $extDecoder,
        array $pending,
        array &$batch,
        int $batchSize,
        array $partitions,
    ): array {
        $stop = false;

        try {
            $rows = $extDecoder->rows($pending)->all();
        } catch (Throwable) {
            $rows = [];

            foreach ($pending as $body) {
                try {
                    $rows[] = $extDecoder->row($body);
                } catch (Throwable) {
                    break;
                }
            }

            $stop = true;
        }

        $ready = [];

        foreach ($rows as $row) {
            $batch[] = $row;

            if (count($batch) === $batchSize) {
                $ready[] = $this->batch($batch, $partitions);
                $batch = [];
            }
        }

        return [$ready, $stop];
    }

    /**
     * Hot path: frames are walked in-buffer, unlike recover() which keeps the
     * reusable FrameReader. offset skips whole leading sections using the footer,
     * then the remaining rows inside the start section; limit stops the read
     * after that many rows.
     *
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows> every batch conforms to the merged file schema
     */
    public function rows(int $batchSize = 1000, int $offset = 0, ?int $limit = null): Generator
    {
        if ($offset < 0) {
            throw new FloeException('Floe offset must be greater or equal to 0');
        }

        if ($limit !== null && $limit < 1) {
            throw new FloeException('Floe limit must be greater than 0');
        }

        if ($offset > 0) {
            yield from $this->rowsFromOffset($batchSize, $offset, $limit);

            return;
        }

        $footer = $this->footer();
        $fileSchema = $footer->fileSchema();

        $partitions = [];

        foreach ($footer->partitions as $name => $value) {
            $partitions[] = new Partition($name, $value);
        }

        /** @var int<1, max> $chunkSize */
        $chunkSize = $this->chunkSize;
        $chunks = ($this->openSource)()->iterate($chunkSize);
        $buffer = '';
        $position = 0;

        $fill = static function (int $bytes) use (&$buffer, &$position, $chunks): bool {
            while ((strlen($buffer) - $position) < $bytes) {
                if (!$chunks->valid()) {
                    return false;
                }

                $buffer .= $chunks->current();
                $chunks->next();
            }

            return true;
        };

        if (!$fill(Format::HEADER_LENGTH)) {
            throw new FloeException('Floe stream is truncated, header is incomplete');
        }

        $flags = Format::validateHeader(substr($buffer, 0, Format::HEADER_LENGTH));

        if ($flags !== $this->codec->id()) {
            throw new FloeException(sprintf(
                'Floe stream was written with codec 0x%02X, expected 0x%02X',
                $flags,
                $this->codec->id(),
            ));
        }

        $position = Format::HEADER_LENGTH;
        $extDecoder = $this->useExtension ? new RowsDecoder() : null;
        $noopCodec = $this->codec instanceof NoopCodec;
        $plan = null;
        $conform = RowPadding::forFileSchema($fileSchema, $this->schemaDecoder);
        $fileSchemaNames = self::schemaColumnNames($fileSchema);
        $fileSchemaCount = count($fileSchemaNames);
        $sectionConforms = false;
        $batch = [];
        $yielded = 0;
        $skip = 0;
        $pending = [];
        $stop = false;
        $flushThreshold = $limit !== null && $limit < $batchSize ? $limit : $batchSize;

        try {
            while (true) {
                if (!$fill(Format::FRAME_HEADER_LENGTH)) {
                    if ((strlen($buffer) - $position) === 0) {
                        break;
                    }

                    throw new FloeException('Floe stream is truncated, frame header is incomplete');
                }

                $frameType = ord($buffer[$position]);
                $frameLength = unpack('V', $buffer, $position + 1)[1];
                $position += Format::FRAME_HEADER_LENGTH;

                if (!$fill($frameLength)) {
                    throw new FloeException('Floe stream is truncated, frame body is incomplete');
                }

                $frameEnd = $position + $frameLength;

                if ($frameType === Format::FRAME_ROW) {
                    if ($extDecoder !== null) {
                        $pending[] = $noopCodec
                            ? substr($buffer, $position, $frameLength)
                            : $this->codec->decode(substr($buffer, $position, $frameLength));
                        $position = $frameEnd;

                        if (count($pending) === $flushThreshold) {
                            foreach ($this->emitExtBatch(
                                $extDecoder,
                                $pending,
                                $conform,
                                $sectionConforms,
                                $fileSchemaCount,
                                $batch,
                                $batchSize,
                                $partitions,
                                $limit,
                                $yielded,
                                $stop,
                                $skip,
                            ) as $ready) {
                                yield $ready;
                            }
                            $pending = [];

                            if ($stop) {
                                return;
                            }
                        }
                    } else {
                        if ($plan === null) {
                            throw new FloeException('Floe found a row frame before any schema frame');
                        }

                        if ($noopCodec) {
                            $row = $this->rowHydrator->hydrate($plan, $buffer, $position);

                            if ($position !== $frameEnd) {
                                throw new FloeException('Floe row frame length does not match its content');
                            }
                        } else {
                            $rowBody = $this->codec->decode(substr($buffer, $position, $frameLength));
                            $bodyPosition = 0;
                            $row = $this->rowHydrator->hydrate($plan, $rowBody, $bodyPosition);

                            if ($bodyPosition !== strlen($rowBody)) {
                                throw new FloeException('Floe row frame length does not match its content');
                            }

                            $position = $frameEnd;
                        }

                        $batch[] = $sectionConforms && $row->entries()->count() === $fileSchemaCount
                            ? $row
                            : $conform->apply($row);

                        if ($limit !== null && ++$yielded >= $limit) {
                            yield $this->batch($batch, $partitions);

                            return;
                        }

                        if (count($batch) === $batchSize) {
                            yield $this->batch($batch, $partitions);
                            $batch = [];
                        }
                    }
                } elseif ($frameType === Format::FRAME_SCHEMA) {
                    $frameBody = substr($buffer, $position, $frameLength);

                    if ($extDecoder !== null) {
                        if ($pending !== []) {
                            foreach ($this->emitExtBatch(
                                $extDecoder,
                                $pending,
                                $conform,
                                $sectionConforms,
                                $fileSchemaCount,
                                $batch,
                                $batchSize,
                                $partitions,
                                $limit,
                                $yielded,
                                $stop,
                                $skip,
                            ) as $ready) {
                                yield $ready;
                            }
                            $pending = [];

                            if ($stop) {
                                return;
                            }
                        }

                        $extDecoder->schema($frameBody);
                    } else {
                        $plan = $this->schemaDecoder->decode($frameBody);
                    }

                    $sectionConforms = self::sectionNames($frameBody) === $fileSchemaNames;
                    $position = $frameEnd;
                } elseif ($frameType === Format::FRAME_PARTITIONS || $frameType === Format::FRAME_FOOTER) {
                    $position = $frameEnd;
                } else {
                    throw new FloeException(sprintf('Floe found unknown frame type 0x%02X', $frameType));
                }

                if ($position >= FrameReader::COMPACT_THRESHOLD) {
                    $buffer = substr($buffer, $position);
                    $position = 0;
                }
            }

            if ($extDecoder !== null && $pending !== []) {
                foreach ($this->emitExtBatch(
                    $extDecoder,
                    $pending,
                    $conform,
                    $sectionConforms,
                    $fileSchemaCount,
                    $batch,
                    $batchSize,
                    $partitions,
                    $limit,
                    $yielded,
                    $stop,
                    $skip,
                ) as $ready) {
                    yield $ready;
                }

                if ($stop) {
                    return;
                }
            }
        } catch (SerializationException|ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }

        if ($batch !== []) {
            yield $this->batch($batch, $partitions);
        }
    }

    /**
     * The first $count rows, without scanning the rest of the file.
     *
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    public function head(int $count, int $batchSize = 1000): Generator
    {
        if ($count < 1) {
            throw new FloeException('Floe head count must be greater than 0');
        }

        return $this->rows($batchSize, 0, $count);
    }

    /**
     * The last $count rows, decoding only from the boundary section onward
     * (footer offsets, no scan). A file with fewer rows yields them all.
     *
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    public function tail(int $count, int $batchSize = 1000): Generator
    {
        if ($count < 1) {
            throw new FloeException('Floe tail count must be greater than 0');
        }

        return $this->rows($batchSize, max(0, $this->totalRows() - $count), null);
    }

    /**
     * Applies the same per-row skip / padding / batch-yield / limit logic as the
     * pure-PHP path; $batch, $yielded, $stop and $skip are updated by reference.
     *
     * @param array<int, string> $pending
     * @param array<int, Row> $batch
     * @param array<int, Partition> $partitions
     * @param int<1, max> $batchSize
     *
     * @throws ExtensionException
     *
     * @return array<int, Rows> completed batches ready to yield
     */
    private function emitExtBatch(
        RowsDecoder $extDecoder,
        array $pending,
        RowPadding $conform,
        bool $sectionConforms,
        int $fileSchemaCount,
        array &$batch,
        int $batchSize,
        array $partitions,
        ?int $limit,
        int &$yielded,
        bool &$stop,
        int &$skip,
    ): array {
        $ready = [];

        foreach ($extDecoder->rows($pending)->all() as $row) {
            if ($skip > 0) {
                $skip--;

                continue;
            }

            $batch[] = $sectionConforms && $row->entries()->count() === $fileSchemaCount ? $row : $conform->apply($row);

            if ($limit !== null && ++$yielded >= $limit) {
                $ready[] = $this->batch($batch, $partitions);
                $batch = [];
                $stop = true;

                return $ready;
            }

            if (count($batch) === $batchSize) {
                $ready[] = $this->batch($batch, $partitions);
                $batch = [];
            }
        }

        return $ready;
    }

    /**
     * @throws FloeException
     */
    public function schema(): Schema
    {
        return $this->footer()->fileSchema();
    }

    /**
     * @throws FloeException
     */
    public function totalRows(): int
    {
        return $this->footer()->totalRows;
    }

    /**
     * @param array<int, Row> $rows
     * @param array<int, Partition> $partitions
     */
    private function batch(array $rows, array $partitions): Rows
    {
        return $partitions === [] ? new Rows(...$rows) : Rows::partitioned($rows, $partitions);
    }

    /**
     * @return \Generator<int, string>
     */
    private function chunksFrom(SourceStream $source, int $startByte, int $size): Generator
    {
        $chunkSize = $this->chunkSize;
        $readAt = $startByte;

        while ($readAt < $size) {
            /** @var int<1, max> $length */
            $length = $chunkSize < ($size - $readAt) ? $chunkSize : $size - $readAt;

            yield $source->read($length, $readAt);

            $readAt += $length;
        }
    }

    /**
     * Offset pushdown: seeks to the start section's byte offset, preloads its
     * schema (its ROWs may have no inline SCHEMA frame when the schema was
     * reused) and skips the remaining rows inside it.
     *
     * @param int<1, max> $batchSize
     * @param int<1, max> $offset
     * @param null|int<1, max> $limit
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    private function rowsFromOffset(int $batchSize, int $offset, ?int $limit): Generator
    {
        $footer = $this->footer();
        $fileSchema = $footer->fileSchema();

        $partitions = [];

        foreach ($footer->partitions as $name => $value) {
            $partitions[] = new Partition($name, $value);
        }

        $cumulative = 0;
        $startSection = null;

        foreach ($footer->sections as $section) {
            if (($cumulative + $section->rowCount) > $offset) {
                $startSection = $section;

                break;
            }

            $cumulative += $section->rowCount;
        }

        if ($startSection === null) {
            return;
        }

        $skip = $offset - $cumulative;
        $schemaBody = $footer->schemaBody($startSection->schemaId);

        $source = ($this->openSource)();
        $size = $source->size();

        if ($size === null) {
            throw new FloeException(sprintf(
                'Floe offset read requires a sized stream, "%s" does not report its size',
                $this->uri,
            ));
        }

        $chunks = $this->chunksFrom($source, $startSection->offset, $size);

        $buffer = '';
        $position = 0;

        $fill = static function (int $bytes) use (&$buffer, &$position, $chunks): bool {
            while ((strlen($buffer) - $position) < $bytes) {
                if (!$chunks->valid()) {
                    return false;
                }

                $buffer .= $chunks->current();
                $chunks->next();
            }

            return true;
        };

        $extDecoder = $this->useExtension ? new RowsDecoder() : null;
        $noopCodec = $this->codec instanceof NoopCodec;

        if ($extDecoder !== null) {
            $extDecoder->schema($schemaBody);
            $plan = null;
        } else {
            $plan = $this->schemaDecoder->decode($schemaBody);
        }

        $conform = RowPadding::forFileSchema($fileSchema, $this->schemaDecoder);
        $fileSchemaNames = self::schemaColumnNames($fileSchema);
        $fileSchemaCount = count($fileSchemaNames);
        $sectionConforms = self::sectionNames($schemaBody) === $fileSchemaNames;
        $batch = [];
        $yielded = 0;
        // skipped rows are still decoded - a corrupt skipped row must throw in strict mode
        $pending = [];
        $stop = false;
        $flushThreshold = $limit !== null && $limit < $batchSize ? $limit : $batchSize;

        try {
            while (true) {
                if (!$fill(Format::FRAME_HEADER_LENGTH)) {
                    if ((strlen($buffer) - $position) === 0) {
                        break;
                    }

                    throw new FloeException('Floe stream is truncated, frame header is incomplete');
                }

                $frameType = ord($buffer[$position]);
                $frameLength = unpack('V', $buffer, $position + 1)[1];
                $position += Format::FRAME_HEADER_LENGTH;

                if (!$fill($frameLength)) {
                    throw new FloeException('Floe stream is truncated, frame body is incomplete');
                }

                $frameEnd = $position + $frameLength;

                if ($frameType === Format::FRAME_ROW) {
                    if ($extDecoder !== null) {
                        $pending[] = $noopCodec
                            ? substr($buffer, $position, $frameLength)
                            : $this->codec->decode(substr($buffer, $position, $frameLength));
                        $position = $frameEnd;

                        if (count($pending) === $flushThreshold) {
                            foreach ($this->emitExtBatch(
                                $extDecoder,
                                $pending,
                                $conform,
                                $sectionConforms,
                                $fileSchemaCount,
                                $batch,
                                $batchSize,
                                $partitions,
                                $limit,
                                $yielded,
                                $stop,
                                $skip,
                            ) as $ready) {
                                yield $ready;
                            }
                            $pending = [];

                            if ($stop) {
                                return;
                            }
                        }
                    } elseif ($plan === null) {
                        throw new FloeException('Floe found a row frame before any schema frame');
                    } else {
                        if ($noopCodec) {
                            $row = $this->rowHydrator->hydrate($plan, $buffer, $position);

                            if ($position !== $frameEnd) {
                                throw new FloeException('Floe row frame length does not match its content');
                            }
                        } else {
                            $rowBody = $this->codec->decode(substr($buffer, $position, $frameLength));
                            $bodyPosition = 0;
                            $row = $this->rowHydrator->hydrate($plan, $rowBody, $bodyPosition);

                            if ($bodyPosition !== strlen($rowBody)) {
                                throw new FloeException('Floe row frame length does not match its content');
                            }

                            $position = $frameEnd;
                        }

                        if ($skip > 0) {
                            $skip--;
                        } else {
                            $batch[] = $sectionConforms && $row->entries()->count() === $fileSchemaCount
                                ? $row
                                : $conform->apply($row);

                            if ($limit !== null && ++$yielded >= $limit) {
                                yield $this->batch($batch, $partitions);

                                return;
                            }

                            if (count($batch) === $batchSize) {
                                yield $this->batch($batch, $partitions);
                                $batch = [];
                            }
                        }
                    }
                } elseif ($frameType === Format::FRAME_SCHEMA) {
                    $frameBody = substr($buffer, $position, $frameLength);

                    if ($extDecoder !== null) {
                        if ($pending !== []) {
                            foreach ($this->emitExtBatch(
                                $extDecoder,
                                $pending,
                                $conform,
                                $sectionConforms,
                                $fileSchemaCount,
                                $batch,
                                $batchSize,
                                $partitions,
                                $limit,
                                $yielded,
                                $stop,
                                $skip,
                            ) as $ready) {
                                yield $ready;
                            }
                            $pending = [];

                            if ($stop) {
                                return;
                            }
                        }

                        $extDecoder->schema($frameBody);
                    } else {
                        $plan = $this->schemaDecoder->decode($frameBody);
                    }

                    $sectionConforms = self::sectionNames($frameBody) === $fileSchemaNames;
                    $position = $frameEnd;
                } elseif ($frameType === Format::FRAME_PARTITIONS || $frameType === Format::FRAME_FOOTER) {
                    $position = $frameEnd;
                } else {
                    throw new FloeException(sprintf('Floe found unknown frame type 0x%02X', $frameType));
                }

                if ($position >= FrameReader::COMPACT_THRESHOLD) {
                    $buffer = substr($buffer, $position);
                    $position = 0;
                }
            }

            if ($extDecoder !== null && $pending !== []) {
                foreach ($this->emitExtBatch(
                    $extDecoder,
                    $pending,
                    $conform,
                    $sectionConforms,
                    $fileSchemaCount,
                    $batch,
                    $batchSize,
                    $partitions,
                    $limit,
                    $yielded,
                    $stop,
                    $skip,
                ) as $ready) {
                    yield $ready;
                }

                if ($stop) {
                    return;
                }
            }
        } catch (SerializationException|ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }

        if ($batch !== []) {
            yield $this->batch($batch, $partitions);
        }
    }

    /**
     * @param null|array<int, \Flow\Floe\HydratorColumn> $plan
     *
     * @throws FloeException
     */
    private function hydrate(?RowsDecoder $extDecoder, ?array $plan, string $rowBody): Row
    {
        if ($extDecoder !== null) {
            return $extDecoder->row($rowBody);
        }

        if ($plan === null) {
            throw new FloeException('Floe found a row frame before any schema frame');
        }

        $position = 0;
        $row = $this->rowHydrator->hydrate($plan, $rowBody, $position);

        if ($position !== strlen($rowBody)) {
            throw new FloeException('Floe row frame length does not match its content');
        }

        return $row;
    }

    /**
     * @return array<int, Partition>
     */
    private static function decodePartitions(string $body): array
    {
        $count = unpack('V', $body)[1];
        $position = 4;
        $partitions = [];

        for ($i = 0; $i < $count; $i++) {
            $nameLength = unpack('V', $body, $position)[1];
            $name = substr($body, $position + 4, $nameLength);
            $position += 4 + $nameLength;
            $valueLength = unpack('V', $body, $position)[1];
            $partitions[] = new Partition($name, substr($body, $position + 4, $valueLength));
            $position += 4 + $valueLength;
        }

        return $partitions;
    }

    /**
     * @return array<int, string> ordered entry names of a SCHEMA frame body
     */
    private static function sectionNames(string $schemaBody): array
    {
        /** @var array<int, array{ref: string}> $definitions */
        $definitions = json_decode($schemaBody, true, 512, JSON_THROW_ON_ERROR);
        $names = [];

        foreach ($definitions as $definition) {
            $names[] = $definition['ref'];
        }

        return $names;
    }

    /**
     * @return array<int, string> ordered column names of a Schema
     */
    private static function schemaColumnNames(Schema $schema): array
    {
        $names = [];

        foreach (array_values($schema->definitions()) as $definition) {
            $names[] = $definition->entry()->name();
        }

        return $names;
    }
}
