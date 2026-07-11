<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Footer;
use Flow\Floe\Format;
use Flow\Floe\FrameReader;
use Flow\Floe\RowsValueMapper;

use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function strlen;
use function substr;

/**
 * Byte-level access to written Floe files, so writer tests can assert the wire
 * layout without going through FloeReader.
 */
final class FloeFileContext
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
     * @return array<int, array{0: int, 1: string}> frame type and body, in file order
     */
    public static function frames(Filesystem $filesystem, Path $path): array
    {
        return iterator_to_array((new FrameReader($filesystem->readFrom($path), 0x00))->frames());
    }

    /**
     * Writes rows to a fresh in-memory Floe file and reads them back as a
     * single batch.
     */
    public static function roundTrip(Rows $rows): Rows
    {
        $filesystem = memory_filesystem();
        $path = path('memory://round-trip.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );

        return $batches === [] ? rows() : $batches[0];
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
     * Reads a written Floe file back into the original Row|Rows the cache stored.
     */
    public static function reconstruct(Filesystem $filesystem, Path $path): Row|Rows
    {
        $file = (new FloeReader($filesystem))->read($path);
        $rows = [];

        foreach ($file->rows() as $batch) {
            foreach ($batch->all() as $row) {
                $rows[] = $row;
            }
        }

        return RowsValueMapper::reconstructFrom($rows, $file->footer());
    }

    /**
     * Reads every row of a Floe file merged into a single Rows.
     */
    public static function readAll(Filesystem $filesystem, Path $path): Rows
    {
        $merged = new Rows();

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->rows() as $batch) {
            $merged = $merged->merge($batch);
        }

        return $merged;
    }

    /**
     * @param array<string, string> $metadata
     */
    public static function write(Filesystem $filesystem, Path $path, Rows $rows, array $metadata = []): void
    {
        $writer = new FloeWriter($filesystem);
        $writer->create($path, Metadata::fromArray($metadata));
        $writer->write($rows);
        $writer->close();
    }

    /**
     * Writes a complete file then rewrites it with the FOOTER frame stripped -
     * a crashed writer that flushed complete frames but never closed. recover()
     * salvages such a file; strict rows() throws on the missing trailer.
     */
    public static function writeWithoutFooter(Filesystem $filesystem, Path $path, Rows $rows): void
    {
        $writer = new FloeWriter($filesystem);
        $writer->create($path);
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
