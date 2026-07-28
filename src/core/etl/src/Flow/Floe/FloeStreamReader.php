<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Generator;

use function count;
use function max;
use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final class FloeStreamReader
{
    /**
     * @var null|Encoder<string>
     */
    private ?Encoder $encoder = null;

    private ?Footer $footer = null;

    private readonly Hydrator $hydrator;

    private readonly SchemaDecoder $schemaDecoder;

    /**
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     *
     * @throws FloeException
     */
    public function __construct(
        private readonly SourceStream $source,
        private readonly Codec $codec,
        private readonly int $chunkSize,
        ?Hydrator $hydrator = null,
        private readonly FloeEngine $engine = FloeEngine::adaptive,
    ) {
        Format::validateCodecId($this->codec->id());
        $this->schemaDecoder = new SchemaDecoder(new ValueDecoder(), new Instantiators());
        $this->hydrator = $hydrator ?? new AdaptiveRowHydrator();
    }

    public function close(): void
    {
        $this->source->close();
    }

    /**
     * @return Encoder<string>
     */
    private function encoder(Schema $schema): Encoder
    {
        return $this->encoder ??= $this->engine->encoder($schema);
    }

    /**
     * @throws FloeException
     */
    public function footer(): Footer
    {
        return $this->footer ??= (new FooterReader())->read($this->source, $this->codec)->footer;
    }

    /**
     * @throws FloeException
     */
    public function metadata(): Metadata
    {
        return $this->footer()->metadata;
    }

    /**
     * Hot path: frames are walked in-buffer. offset skips whole leading sections
     * using the footer, then the remaining rows inside the start section; limit
     * stops the read after that many rows.
     *
     * @param int<1, max> $batchSize
     * @param bool $conform pad/reorder every batch to the merged file schema (default); false yields rows verbatim per section (the raw read used by spill buckets)
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows> every batch conforms to the merged file schema unless $conform is false
     */
    public function rows(int $batchSize = 1000, int $offset = 0, ?int $limit = null, bool $conform = true): Generator
    {
        if ($offset < 0) {
            throw new FloeException('Floe offset must be greater or equal to 0');
        }

        if ($limit !== null && $limit < 1) {
            throw new FloeException('Floe limit must be greater than 0');
        }

        if ($offset > 0) {
            yield from $this->rowsFromOffset($batchSize, $offset, $limit, $conform);

            return;
        }

        $footer = $this->footer();
        $fileSchema = $footer->schema();

        /** @var int<1, max> $chunkSize */
        $chunkSize = $this->chunkSize;
        $chunks = $this->source->iterate($chunkSize);
        $buffer = '';
        $position = 0;

        $fill = FrameReader::chunkFiller($buffer, $position, $chunks);

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

        yield from $this->walk(
            $chunks,
            $buffer,
            Format::HEADER_LENGTH,
            $fileSchema,
            0,
            [],
            $conform ? RowPadding::forFileSchema($fileSchema, $this->schemaDecoder) : null,
            $fileSchema->count(),
            $batchSize,
            $limit,
        );
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
     * The single strict frame-walk shared by rows() and rowsFromOffset(); they
     * differ only in how it is seeded - start position, skip and partitions. The
     * file carries exactly one schema, so decode state comes from the footer.
     *
     * @param \Generator<int, string> $chunks
     * @param array<int, Partition> $partitions
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    private function walk(
        Generator $chunks,
        string $buffer,
        int $position,
        Schema $schema,
        int $skip,
        array $partitions,
        ?RowPadding $padding,
        int $fileSchemaCount,
        int $batchSize,
        ?int $limit,
    ): Generator {
        $fill = FrameReader::chunkFiller($buffer, $position, $chunks);

        $batch = [];
        $yielded = 0;
        /** @var list<string> $pending */
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
                    $pending[] = $this->codec->decode(substr($buffer, $position, $frameLength));
                    $position = $frameEnd;

                    if (count($pending) === $flushThreshold) {
                        foreach ($this->emitBatch(
                            $schema,
                            $pending,
                            $padding,
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
                } elseif ($frameType === Format::FRAME_PARTITIONS) {
                    $frameBody = substr($buffer, $position, $frameLength);

                    if ($pending !== []) {
                        foreach ($this->emitBatch(
                            $schema,
                            $pending,
                            $padding,
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

                    if ($batch !== []) {
                        yield $this->batch($batch, $partitions);
                        $batch = [];
                    }

                    $partitions = self::decodePartitions($frameBody);
                    $position = $frameEnd;
                } elseif ($frameType === Format::FRAME_FOOTER) {
                    $position = $frameEnd;
                } else {
                    throw new FloeException(sprintf('Floe found unknown frame type 0x%02X', $frameType));
                }

                if ($position >= FrameReader::COMPACT_THRESHOLD) {
                    $buffer = substr($buffer, $position);
                    $position = 0;
                }
            }

            if ($pending !== []) {
                foreach ($this->emitBatch(
                    $schema,
                    $pending,
                    $padding,
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
     * Applies the per-row skip / padding / batch-yield / limit logic to a hydrated
     * batch of row frame bodies; $batch, $yielded, $stop and $skip are updated by reference.
     *
     * @param list<string> $pending
     * @param array<int, Row> $batch
     * @param array<int, Partition> $partitions
     * @param int<1, max> $batchSize
     *
     * @return array<int, Rows> completed batches ready to yield
     */
    private function emitBatch(
        Schema $schema,
        array $pending,
        ?RowPadding $padding,
        int $fileSchemaCount,
        array &$batch,
        int $batchSize,
        array $partitions,
        ?int $limit,
        int &$yielded,
        bool &$stop,
        int &$skip,
    ): array {
        return $this->emitRows(
            $this->hydrator->hydrate($this->encoder($schema)->decode($pending), $schema)->all(),
            $padding,
            $fileSchemaCount,
            $batch,
            $batchSize,
            $partitions,
            $limit,
            $yielded,
            $stop,
            $skip,
        );
    }

    /**
     * Per-row skip / padding / batch-yield / limit logic over already-hydrated
     * rows; $batch, $yielded, $stop and $skip are updated by reference. A null
     * $padding yields rows verbatim (per-section, unpadded) - the raw read used
     * by external-sort/join/group-by buckets.
     *
     * @param array<array-key, Row> $rows
     * @param array<int, Row> $batch
     * @param array<int, Partition> $partitions
     * @param int<1, max> $batchSize
     *
     * @return array<int, Rows> completed batches ready to yield
     */
    private function emitRows(
        array $rows,
        ?RowPadding $padding,
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

        foreach ($rows as $row) {
            if ($skip > 0) {
                $skip--;

                continue;
            }

            $batch[] =
                $padding === null || $row->entries()->count() === $fileSchemaCount ? $row : $padding->apply($row);

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
        return $this->footer()->schema();
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
     * Offset pushdown: seeks to the start section's byte offset, takes the
     * schema from the footer and skips the remaining rows inside the start
     * section.
     *
     * @param int<1, max> $batchSize
     * @param int<1, max> $offset
     * @param null|int<1, max> $limit
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    private function rowsFromOffset(int $batchSize, int $offset, ?int $limit, bool $conform = true): Generator
    {
        $footer = $this->footer();
        $fileSchema = $footer->schema();

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

        $partitions = [];

        foreach ($footer->partitionsFor($startSection->partitionsId) as $name => $value) {
            $partitions[] = new Partition($name, $value);
        }

        $size = $this->source->size();

        if ($size === null) {
            throw new FloeException(sprintf(
                'Floe offset read requires a sized stream, "%s" does not report its size',
                $this->source->path()->uri(),
            ));
        }

        yield from $this->walk(
            $this->chunksFrom($this->source, $startSection->offset, $size),
            '',
            0,
            $fileSchema,
            $offset - $cumulative,
            $partitions,
            $conform ? RowPadding::forFileSchema($fileSchema, $this->schemaDecoder) : null,
            $fileSchema->count(),
            $batchSize,
            $limit,
        );
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
}
