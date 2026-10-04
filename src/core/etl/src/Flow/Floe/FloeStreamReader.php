<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Generator;

use function max;
use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final class FloeStreamReader
{
    private ?Footer $footer = null;

    private ?Schema $schema = null;

    private readonly FrameDecoder $frameDecoder;

    /**
     * @throws FloeException
     */
    public function __construct(
        private readonly SourceStream $source,
        private readonly Codec $codec,
        private readonly int $chunkSize,
        private readonly Backend $backend,
    ) {
        Format::validateCodecId($this->codec->id());
        $this->frameDecoder = new FrameDecoder($this->codec);
    }

    public function close(): void
    {
        $this->source->close();
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
     * stops the read after that many rows. Batches are frame-sized: batchSize is
     * an upper bound, a batch never spans two BATCH frames.
     *
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows> every batch carries the merged file schema
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

        yield from $this->walk($chunks, $buffer, Format::HEADER_LENGTH, $fileSchema, 0, $batchSize, $limit);
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
     * differ only in how it is seeded - start position and skip. The
     * file carries exactly one schema, so decode state comes from the footer.
     * Every BATCH frame yields at most batchSize rows per batch; a frame wholly
     * inside skip is passed over without decoding.
     *
     * @param \Generator<int, string> $chunks
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
        int $batchSize,
        ?int $limit,
    ): Generator {
        $fill = FrameReader::chunkFiller($buffer, $position, $chunks);

        $yielded = 0;

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

                if ($frameType === Format::FRAME_BATCH) {
                    if ($frameLength < 4) {
                        throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
                    }

                    /** @var int $rowCount */
                    $rowCount = unpack('V', $buffer, $position)[1];

                    if ($skip >= $rowCount) {
                        $skip -= $rowCount;
                    } else {
                        $rows = $this->frameDecoder->decode(
                            substr($buffer, $position, $frameLength),
                            $schema,
                            $this->backend,
                        );

                        if ($skip > 0) {
                            $rows = $rows->slice($skip, $rows->count() - $skip);
                            $skip = 0;
                        }

                        if ($limit !== null && $rows->count() > ($limit - $yielded)) {
                            $rows = $rows->slice(0, $limit - $yielded);
                        }

                        if ($rows->count() <= $batchSize) {
                            yield $rows;
                        } else {
                            foreach ($rows->chunks($batchSize) as $chunk) {
                                yield $chunk;
                            }
                        }

                        $yielded += $rows->count();

                        if ($limit !== null && $yielded === $limit) {
                            return;
                        }
                    }

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
        } catch (SerializationException|RuntimeException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    /**
     * @throws FloeException
     */
    public function schema(): Schema
    {
        return $this->schema ??= $this->footer()->schema();
    }

    /**
     * Uncompressed bytes of the data frames, as the footer records them.
     *
     * @throws FloeException
     */
    public function byteSize(): int
    {
        return $this->footer()->statistics->byteSize;
    }

    /**
     * @throws FloeException
     */
    public function totalRows(): int
    {
        return $this->footer()->statistics->rows;
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
    private function rowsFromOffset(int $batchSize, int $offset, ?int $limit): Generator
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
            $batchSize,
            $limit,
        );
    }
}
