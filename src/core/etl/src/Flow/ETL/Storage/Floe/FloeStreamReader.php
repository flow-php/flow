<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

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

use function max;
use function min;
use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final class FloeStreamReader
{
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
     * offset skips whole leading sections using the footer, then the remaining rows inside the start section; limit
     * stops the read after that many rows. Batches are frame-sized: batchSize is an upper bound, a batch never spans
     * two BATCH frames.
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

        // footer() validated the header and refused an unsized stream, so the walk starts past the header
        $fileSchema = $this->footer()->schema();

        yield from $this->walk(Format::HEADER_LENGTH, $this->source->size() ?? 0, $fileSchema, 0, $batchSize, $limit);
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @throws FloeException
     *
     * @return \Generator<int, Rows>
     */
    private function walk(int $at, int $size, Schema $schema, int $skip, int $batchSize, ?int $limit): Generator
    {
        $window = '';
        $windowAt = $at;
        // one read per miss: a remote stream gets no more range requests than chunk-sized reads would send
        $hold = function (int $from, int $bytes) use (&$window, &$windowAt, $size): bool {
            if (($from + $bytes) <= ($windowAt + strlen($window))) {
                return true;
            }

            if (($from + $bytes) > $size) {
                return false;
            }

            if ($bytes >= $this->chunkSize) {
                // a frame of a chunk or more is read whole, so decodeFrame() takes the read string without a copy
                $window = $this->source->read(max(1, $bytes), $from);
            } else {
                $end = $windowAt + strlen($window);
                /** @var int<1, max> $length */
                $length = min($this->chunkSize, $size - $end);
                $window = substr($window, $from - $windowAt) . $this->source->read($length, $end);
            }

            $windowAt = $from;

            return strlen($window) >= $bytes;
        };

        $yielded = 0;

        try {
            while ($at < $size) {
                if (!$hold($at, Format::FRAME_HEADER_LENGTH)) {
                    throw new FloeException('Floe stream is truncated, frame header is incomplete');
                }

                $frameType = ord($window[$at - $windowAt]);
                $frameLength = unpack('V', $window, $at - $windowAt + 1)[1];
                $body = $at + Format::FRAME_HEADER_LENGTH;

                if (!$hold($body, $frameLength)) {
                    throw new FloeException('Floe stream is truncated, frame body is incomplete');
                }

                $position = $body - $windowAt;

                if ($frameType === Format::FRAME_BATCH) {
                    if ($frameLength < 4) {
                        throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
                    }

                    /** @var int $rowCount */
                    $rowCount = unpack('V', $window, $position)[1];

                    if ($skip >= $rowCount) {
                        $skip -= $rowCount;
                    } else {
                        $rows = $this->backend->decodeFrame(
                            $position === 0 && strlen($window) === $frameLength
                                ? $window
                                : substr($window, $position, $frameLength),
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
                } elseif ($frameType !== Format::FRAME_FOOTER) {
                    throw new FloeException(sprintf('Floe found unknown frame type 0x%02X', $frameType));
                }

                $at = $body + $frameLength;
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

        yield from $this->walk($startSection->offset, $size, $fileSchema, $offset - $cumulative, $batchSize, $limit);
    }
}
