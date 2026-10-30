<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Closure;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Storage\Serializer\Exception\SerializationException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Filesystem\SourceStream;
use Generator;

use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final class FloeStreamReader
{
    public const int COMPACT_THRESHOLD = 1_048_576;

    private ?Footer $footer = null;

    private ?Schema $schema = null;

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
    }

    /**
     * @param Generator<int, string> $chunks
     *
     * @return Closure(int): bool
     */
    public static function chunkFiller(string &$buffer, int &$position, Generator $chunks): Closure
    {
        return static function (int $bytes) use (&$buffer, &$position, $chunks): bool {
            while ((strlen($buffer) - $position) < $bytes) {
                if (!$chunks->valid()) {
                    return false;
                }

                $buffer .= $chunks->current();
                $chunks->next();
            }

            return true;
        };
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

        // footer() already validated the header, so the walk starts past its bytes
        $fileSchema = $this->footer()->schema();

        /** @var int<1, max> $chunkSize */
        $chunkSize = $this->chunkSize;

        yield from $this->walk(
            $this->source->iterate($chunkSize),
            '',
            Format::HEADER_LENGTH,
            $fileSchema,
            0,
            $batchSize,
            $limit,
        );
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
        $fill = self::chunkFiller($buffer, $position, $chunks);

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
                        $rows = $this->backend->decodeFrame(
                            substr($buffer, $position, $frameLength),
                            $schema,
                            $this->codec,
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

                if ($position >= self::COMPACT_THRESHOLD) {
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
    public function totalRows(): int
    {
        return $this->footer()->statistics->rows;
    }

    /**
     * @throws FloeException
     */
    public function schema(): Schema
    {
        return $this->schema ??= $this->footer()->schema();
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
