<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;

use function count;
use function sprintf;
use function strlen;

final readonly class FloeMerger
{
    private const int COPY_CHUNK_SIZE = 65_536;

    public function __construct(
        private Filesystem $filesystem,
        private ?bool $useExtension = null,
    ) {}

    /**
     * @param array<int, Path> $sources
     * @param ?Metadata $metadata merged over the sources' footer metadata, new keys win
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public function merge(array $sources, Path $dest, bool $compact = false, ?Metadata $metadata = null): void
    {
        if ($sources === []) {
            throw new FloeException('Floe merge requires at least one source file');
        }

        $reconciled = $this->reconcile($sources);

        $metadata ??= Metadata::empty();

        if ($compact) {
            $this->mergeCompact($sources, $reconciled['layouts'], $dest, $metadata);

            return;
        }

        $this->mergeSplice(
            $sources,
            $reconciled['layouts'],
            $reconciled['merged'],
            $reconciled['partitions'],
            $dest,
            $metadata,
        );
    }

    /**
     * Reads every source's footer/layout and validates the merge is possible:
     * one shared partition combination and a schema that evolves cleanly.
     *
     * @param array<int, Path> $sources
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     *
     * @return array{layouts: array<int, array{footer: Footer, footerFrameStart: int, partitionsFrameLength: int}>, merged: null|Schema, partitions: array<string, string>}
     */
    private function reconcile(array $sources): array
    {
        $layouts = [];
        $merged = null;
        $partitions = null;

        foreach ($sources as $index => $source) {
            $layout = $this->readLayout($source);
            $footer = $layout['footer'];

            if ($partitions === null) {
                $partitions = $footer->partitions;
            } elseif ($footer->partitions !== $partitions) {
                throw new IncompatibleSchemaException(sprintf(
                    'Floe merge requires all sources to share one partition combination, "%s" differs',
                    $source->uri(),
                ));
            }

            $schema = $footer->fileSchema();

            if ($merged !== null) {
                $this->assertCompatible($merged, $schema, $source);
            }

            $merged = $merged === null ? $schema : $merged->merge($schema);
            $layouts[$index] = $layout;
        }

        return ['layouts' => $layouts, 'merged' => $merged, 'partitions' => $partitions ?? []];
    }

    /**
     * Columns shared with the running merged schema must keep a compatible type;
     * columns present in only some sources are fine - Schema::merge auto-nullables
     * them and the reader pads the sections that lack them.
     *
     * @throws IncompatibleSchemaException
     */
    private function assertCompatible(Schema $merged, Schema $incoming, Path $source): void
    {
        foreach ($incoming->definitions() as $definition) {
            $existing = $merged->findDefinition($definition->entry()->name());

            if ($existing !== null && !$existing->isCompatible($definition)) {
                throw new IncompatibleSchemaException(sprintf(
                    'Floe merge cannot reconcile column "%s" of "%s": %s is not compatible with %s',
                    $definition->entry()->name(),
                    $source->uri(),
                    $definition->type()->toString(),
                    $existing->type()->toString(),
                ));
            }
        }
    }

    /**
     * @param array<int, Path> $sources
     * @param array<int, array{footer: Footer, footerFrameStart: int, partitionsFrameLength: int}> $layouts
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    private function mergeCompact(array $sources, array $layouts, Path $dest, Metadata $metadata): void
    {
        $mergedMetadata = Metadata::empty();

        foreach ($layouts as $layout) {
            $mergedMetadata = $mergedMetadata->merge($layout['footer']->metadata);
        }

        $writer = new FloeWriter($this->filesystem, useExtension: $this->useExtension);
        $writer->create($dest, $mergedMetadata->merge($metadata));

        $reader = new FloeReader($this->filesystem, useExtension: $this->useExtension);

        foreach ($sources as $source) {
            foreach ($reader->read($source)->rows() as $batch) {
                $writer->write($batch);
            }
        }

        $writer->close();
    }

    /**
     * @param array<int, Path> $sources
     * @param array<int, array{footer: Footer, footerFrameStart: int, partitionsFrameLength: int}> $layouts
     * @param array<string, string> $partitions
     *
     * @throws FloeException
     */
    private function mergeSplice(
        array $sources,
        array $layouts,
        ?Schema $merged,
        array $partitions,
        Path $dest,
        Metadata $metadata,
    ): void {
        /** @var array<int, array<int, array<string, mixed>>> $schemas */
        $schemas = [];
        $sections = [];
        $totalRows = 0;
        $mergedMetadata = Metadata::empty();

        $stream = $this->filesystem->writeTo($dest);
        $stream->append(Format::header(0x00));
        $destPosition = Format::HEADER_LENGTH;

        foreach ($sources as $index => $source) {
            $layout = $layouts[$index];
            $footer = $layout['footer'];
            $copyStart = $index === 0
                ? Format::HEADER_LENGTH
                : Format::HEADER_LENGTH + $layout['partitionsFrameLength'];
            $regionLength = $layout['footerFrameStart'] - $copyStart;
            $outputStart = $destPosition;

            $schemaIdMap = $this->mergeSchemas($footer->schemas, $schemas);

            if ($regionLength > 0) {
                $sourceStream = $this->filesystem->readFrom($source);
                $at = $copyStart;
                $remaining = $regionLength;

                while ($remaining > 0) {
                    /** @var int<1, max> $length */
                    $length = $remaining < self::COPY_CHUNK_SIZE ? $remaining : self::COPY_CHUNK_SIZE;
                    $stream->append($sourceStream->read($length, $at));
                    $at += $length;
                    $remaining -= $length;
                    $destPosition += $length;
                }

                $sourceStream->close();
            }

            foreach ($footer->sections as $section) {
                $sections[] = new Section(
                    $section->offset - $copyStart + $outputStart,
                    $schemaIdMap[$section->schemaId],
                    $section->rowCount,
                );
            }

            $totalRows += $footer->totalRows;
            $mergedMetadata = $mergedMetadata->merge($footer->metadata);
        }

        /** @var array<int, array<string, mixed>> $fileSchema */
        $fileSchema = $merged?->normalize() ?? [];

        $footerJson = (new Footer(
            Format::VERSION,
            FloeWriter::writerVersion(),
            $schemas,
            $fileSchema,
            $sections,
            $partitions,
            $totalRows,
            $mergedMetadata->merge($metadata),
        ))->toJson();

        $stream->append(Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))));
        $stream->close();
    }

    /**
     * Adds a source's schemas to the combined table (dedup by equality) and
     * returns its old->new schema id mapping.
     *
     * @param array<int, array<int, array<string, mixed>>> $sourceSchemas
     * @param array<int, array<int, array<string, mixed>>> $schemas combined table, updated in place
     *
     * @return array<int, int>
     */
    private function mergeSchemas(array $sourceSchemas, array &$schemas): array
    {
        $map = [];

        foreach ($sourceSchemas as $oldId => $decoded) {
            $newId = null;

            foreach ($schemas as $id => $known) {
                if ($known == $decoded) {
                    $newId = $id;

                    break;
                }
            }

            if ($newId === null) {
                $newId = count($schemas);
                $schemas[] = $decoded;
            }

            $map[$oldId] = $newId;
        }

        return $map;
    }

    /**
     * @throws FloeException
     *
     * @return array{footer: Footer, footerFrameStart: int, partitionsFrameLength: int}
     */
    private function readLayout(Path $source): array
    {
        if ($this->filesystem->status($source) === null) {
            throw new FloeException(sprintf('Floe merge source "%s" does not exist', $source->uri()));
        }

        $stream = $this->filesystem->readFrom($source);
        $size = $stream->size();

        if ($size === null) {
            $stream->close();

            throw new FloeException(sprintf(
                'Floe merge requires sized sources, "%s" does not report its size',
                $source->uri(),
            ));
        }

        if ($size < (Format::HEADER_LENGTH + Format::TRAILER_LENGTH)) {
            $stream->close();

            throw new FloeException(sprintf(
                'Floe merge source "%s" is torn, too small to hold a header and a trailer',
                $source->uri(),
            ));
        }

        $flags = Format::validateHeader($stream->read(Format::HEADER_LENGTH, 0));

        if ($flags !== 0x00) {
            $stream->close();

            throw new FloeException(sprintf(
                'Floe merge supports only the no-op codec, source "%s" uses codec 0x%02X',
                $source->uri(),
                $flags,
            ));
        }

        $footerLength = Format::parseTrailer($stream->read(Format::TRAILER_LENGTH, $size - Format::TRAILER_LENGTH));
        $footerFrameStart = $size - Format::TRAILER_LENGTH - $footerLength - Format::FRAME_HEADER_LENGTH;

        if ($footerFrameStart < Format::HEADER_LENGTH) {
            $stream->close();

            throw new FloeException(sprintf(
                'Floe merge source "%s" is torn, footer does not fit inside the file',
                $source->uri(),
            ));
        }

        /** @var int<1, max> $footerLength */
        $footer = Footer::fromJson($stream->read($footerLength, $size - Format::TRAILER_LENGTH - $footerLength));
        $stream->close();

        return [
            'footer' => $footer,
            'footerFrameStart' => $footerFrameStart,
            'partitionsFrameLength' => $this->partitionsFrameLength($footer->partitions),
        ];
    }

    /**
     * Byte length of the leading PARTITIONS frame (0 when not partitioned),
     * computed from the footer so no frame walk is needed. Order-independent:
     * only the summed name/value lengths matter.
     *
     * @param array<string, string> $partitions
     */
    private function partitionsFrameLength(array $partitions): int
    {
        if ($partitions === []) {
            return 0;
        }

        $bodyLength = 4;

        foreach ($partitions as $name => $value) {
            $bodyLength += 4 + strlen($name) + 4 + strlen($value);
        }

        return Format::FRAME_HEADER_LENGTH + $bodyLength;
    }
}
