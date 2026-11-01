<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Storage\Floe\Footer;
use Flow\ETL\Storage\Floe\FooterReader;
use Flow\ETL\Storage\Floe\FooterStatistics;
use Flow\ETL\Storage\Floe\Format;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Storage\Floe\Section;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function ord;
use function strlen;
use function substr;
use function unpack;

/**
 * Byte-level access to written Floe files, so writer tests can assert the wire
 * layout without going through FloeFiles.
 */
final class FloeStreamReaderContext
{
    public static function footer(Filesystem $filesystem, Path $path): Footer
    {
        $source = $filesystem->readFrom($path);
        $size = (int) $source->size();
        /** @var int<1, max> $footerLength */
        $footerLength = Format::parseTrailer($source->read(Format::TRAILER_LENGTH, $size - Format::TRAILER_LENGTH));

        return Footer::fromJson($source->read($footerLength, $size - Format::TRAILER_LENGTH - $footerLength));
    }

    /**
     * The bytes between the header and the FOOTER frame - what the footer's byteSize has to report.
     */
    public static function dataBytes(Filesystem $filesystem, Path $path): int
    {
        return (
            (new FooterReader())->read($filesystem->readFrom($path), new NoopCodec())->footerFrameStart
            - Format::HEADER_LENGTH
        );
    }

    /**
     * @return array<int, array{0: int, 1: string}> frame type and body, in file order
     */
    public static function frames(Filesystem $filesystem, Path $path): array
    {
        $bytes = $filesystem->readFrom($path)->content();
        $frames = [];

        for ($position = Format::HEADER_LENGTH; $position < strlen($bytes);) {
            $length = unpack('V', $bytes, $position + 1)[1];
            $frames[] = [ord($bytes[$position]), substr($bytes, $position + Format::FRAME_HEADER_LENGTH, $length)];
            $position += Format::FRAME_HEADER_LENGTH + $length;
        }

        return $frames;
    }

    /**
     * Writes rows to a fresh in-memory Floe file and reads them back as a
     * single batch.
     */
    public static function roundTrip(Rows $rows): Rows
    {
        $filesystem = memory_filesystem();
        $path = path('memory://round-trip.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $rows->schema());
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );

        return $batches === [] ? rows(schema()) : $batches[0];
    }

    /**
     * @return array<int, int> frame types in file order
     */
    public static function frameTypes(Filesystem $filesystem, Path $path): array
    {
        $types = [];

        foreach (self::frames($filesystem, $path) as [$type]) {
            $types[] = $type;
        }

        return $types;
    }

    /**
     * Reads a written Floe file back into the original Rows the cache stored.
     */
    public static function reconstruct(Filesystem $filesystem, Path $path): Rows
    {
        $file = (new FloeFiles($filesystem, new AdaptiveBackend()))->read($path);
        $rows = rows($file->footer()->schema());

        foreach ($file->rows() as $batch) {
            $rows = Rows::concat(
                $rows->schema(),
                new AdaptiveBackend(),
                $rows,
                $batch->matchTo($file->footer()->schema(), new AdaptiveBackend()),
            );
        }

        return $rows;
    }

    /**
     * Reads every row of a Floe file merged into a single Rows.
     */
    public static function readAll(Filesystem $filesystem, Path $path): Rows
    {
        $merged = null;

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->rows() as $batch) {
            $merged = $merged === null ? $batch : Rows::concat($merged->schema(), new PhpBackend(), $merged, $batch);
        }

        return $merged ?? rows(schema());
    }

    /**
     * @param array<string, string> $metadata
     */
    public static function write(
        Filesystem $filesystem,
        Path $path,
        Rows $rows,
        array $metadata = [],
        Codec $codec = new NoopCodec(),
    ): void {
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: $codec)))->create(
            $path,
            $rows->schema(),
            Metadata::fromArray($metadata),
        );
        $writer->write($rows);
        $writer->close();
    }

    /**
     * A file holding exactly these complete frames under `$schema` - frames no FloeStreamWriter would produce.
     */
    public static function writeFrames(
        Filesystem $filesystem,
        Path $path,
        Schema $schema,
        string $frames,
        int $rowCount,
    ): void {
        /** @var array<int, array<string, mixed>> $normalized */
        $normalized = $schema->normalize();
        $footerJson = (new Footer(
            Format::VERSION,
            'test',
            $normalized,
            [new Section(Format::HEADER_LENGTH, $rowCount)],
            new FooterStatistics($rowCount, strlen($frames)),
            Metadata::empty(),
        ))->toJson();

        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . $frames
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();
    }

    /**
     * @param list<Rows> $batches
     */
    public static function writeBatches(
        Filesystem $filesystem,
        Path $path,
        Schema $schema,
        array $batches,
        Options $options = new Options(),
    ): string {
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend(), $options))->create($path, $schema);

        foreach ($batches as $batch) {
            $writer->write($batch);
        }

        $writer->close();

        return $filesystem->readFrom($path)->content();
    }

    public static function writeWithoutFooter(Filesystem $filesystem, Path $path, Rows $rows): void
    {
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $rows->schema());
        $writer->write($rows);
        $writer->close();

        $content = $filesystem->readFrom($path)->content();
        /** @var int<1, max> $footerLength */
        $footerLength = Format::parseTrailer(substr($content, -Format::TRAILER_LENGTH));
        // FOOTER frame = 5-byte frame header + footer JSON + 8-byte trailer.
        $torn = substr($content, 0, strlen($content) - (5 + $footerLength + Format::TRAILER_LENGTH));

        $filesystem->writeTo($path)->append($torn)->close();
    }
}
