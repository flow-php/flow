<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Hydrator;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Schema\Validator\EvolvingValidator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;

use function count;
use function sprintf;

final readonly class FloeMerger
{
    private const int COPY_CHUNK_SIZE = 65_536;

    public function __construct(
        private Filesystem $filesystem,
        private ?Hydrator $hydrator = null,
        private Codec $codec = new NoopCodec(),
    ) {
        Format::validateCodecId($this->codec->id());
    }

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

        // a merged file carries exactly one schema; raw splicing is only valid when every
        // source already shares it - differing (but compatible) schemas are re-encoded to
        // the union, exactly as compaction does
        if ($compact || !$this->sourcesShareSchema($reconciled['layouts'], $reconciled['merged'])) {
            $this->mergeCompact($sources, $reconciled['layouts'], $reconciled['merged'], $dest, $metadata);

            return;
        }

        $this->mergeSplice($sources, $reconciled['layouts'], $reconciled['merged'], $dest, $metadata);
    }

    /**
     * Splicing copies source row frames verbatim, so it is only valid when every
     * source's schema is structurally identical to the merged footer schema -
     * column order included, since row bytes are laid out in schema order.
     * Sources that merely reconcile (isSame is order-insensitive) are re-encoded
     * by the compact path instead.
     *
     * @param array<int, array{footer: Footer, footerFrameStart: int}> $layouts
     */
    private function sourcesShareSchema(array $layouts, ?Schema $merged): bool
    {
        if ($merged === null) {
            return true;
        }

        $mergedNormalized = $merged->normalize();

        foreach ($layouts as $layout) {
            $schema = $layout['footer']->schema();

            if ($schema->count() === 0) {
                continue;
            }

            if ($schema->normalize() !== $mergedNormalized) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param array<int, Path> $sources
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     *
     * @return array{layouts: array<int, array{footer: Footer, footerFrameStart: int}>, merged: null|Schema}
     */
    private function reconcile(array $sources): array
    {
        $layouts = [];
        $merged = null;

        foreach ($sources as $index => $source) {
            $layout = $this->readLayout($source);
            $layouts[$index] = $layout;
            $schema = $layout['footer']->schema();

            // an empty (zero-row) source has no rows and no columns to reconcile
            if ($schema->count() === 0) {
                continue;
            }

            if ($merged !== null) {
                $validation = (new EvolvingValidator())->validate($merged, $schema);

                if (!$validation->isValid()) {
                    throw new IncompatibleSchemaException(sprintf(
                        'Floe merge cannot reconcile the schema of "%s":%s',
                        $source->uri(),
                        $validation->toString(),
                    ));
                }
            }

            $merged = $merged === null ? $schema : $merged->merge($schema);
        }

        return ['layouts' => $layouts, 'merged' => $merged];
    }

    /**
     * @param array<int, Path> $sources
     * @param array<int, array{footer: Footer, footerFrameStart: int}> $layouts
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    private function mergeCompact(array $sources, array $layouts, ?Schema $merged, Path $dest, Metadata $metadata): void
    {
        $mergedMetadata = Metadata::empty();

        foreach ($layouts as $layout) {
            $mergedMetadata = $mergedMetadata->merge($layout['footer']->metadata);
        }

        $writer = new FloeWriter(
            $this->filesystem,
            $merged ?? new Schema(),
            new Options(codec: $this->codec),
            hydrator: $this->hydrator,
        );
        $writer->create($dest, $mergedMetadata->merge($metadata));

        $reader = new FloeReader($this->filesystem, $this->codec, hydrator: $this->hydrator);

        foreach ($sources as $source) {
            foreach ($reader->read($source)->rows() as $batch) {
                $writer->write($batch);
            }
        }

        $writer->close();
    }

    /**
     * @param array<int, Path> $sources
     * @param array<int, array{footer: Footer, footerFrameStart: int}> $layouts
     *
     * @throws FloeException
     */
    private function mergeSplice(array $sources, array $layouts, ?Schema $merged, Path $dest, Metadata $metadata): void
    {
        /** @var array<int, array<string, string>> $partitions */
        $partitions = [];
        $sections = [];
        $totalRows = 0;
        $mergedMetadata = Metadata::empty();
        $runningCombo = [];

        $frameWriter = new FrameWriter($this->filesystem->writeTo($dest), $this->codec->id(), self::COPY_CHUNK_SIZE);
        $frameWriter->header();

        foreach ($sources as $index => $source) {
            $layout = $layouts[$index];
            $footer = $layout['footer'];
            $copyStart = Format::HEADER_LENGTH;
            $regionLength = $layout['footerFrameStart'] - $copyStart;

            $partitionsIdMap = $this->mergePartitions($footer->partitions, $partitions);

            $firstCombo = $footer->sections === [] ? [] : $footer->partitionsFor($footer->sections[0]->partitionsId);

            if ($firstCombo !== $runningCombo && $firstCombo === []) {
                $frameWriter->partitions($firstCombo);
            }

            $outputStart = $frameWriter->position();

            if ($regionLength > 0) {
                $sourceStream = $this->filesystem->readFrom($source);
                $at = $copyStart;
                $remaining = $regionLength;

                while ($remaining > 0) {
                    /** @var int<1, max> $length */
                    $length = $remaining < self::COPY_CHUNK_SIZE ? $remaining : self::COPY_CHUNK_SIZE;
                    $frameWriter->raw($sourceStream->read($length, $at));
                    $at += $length;
                    $remaining -= $length;
                }

                $sourceStream->close();
            }

            foreach ($footer->sections as $section) {
                $sections[] = new Section(
                    $section->offset - $copyStart + $outputStart,
                    $partitionsIdMap[$section->partitionsId],
                    $section->rowCount,
                );
            }

            if ($footer->sections !== []) {
                $runningCombo = $footer->partitionsFor($footer->sections[count($footer->sections) - 1]->partitionsId);
            }

            $totalRows += $footer->totalRows;
            $mergedMetadata = $mergedMetadata->merge($footer->metadata);
        }

        /** @var array<int, array<string, mixed>> $schema */
        $schema = $merged?->normalize() ?? [];

        $footerJson = (new Footer(
            Format::VERSION,
            FloeStreamWriter::writerVersion(),
            $schema,
            $sections,
            $partitions,
            $totalRows,
            $mergedMetadata->merge($metadata),
        ))->toJson();

        $frameWriter->footer($footerJson);
        $frameWriter->close();
    }

    /**
     * Adds a source's partition combinations to the merged table (order-sensitive
     * dedup) and returns its old->new partitionsId mapping.
     *
     * @param array<int, array<string, string>> $sourceTable
     * @param array<int, array<string, string>> $table combined table, updated in place
     *
     * @return array<int, int>
     */
    private function mergePartitions(array $sourceTable, array &$table): array
    {
        $map = [];

        foreach ($sourceTable as $oldId => $combo) {
            $newId = null;

            foreach ($table as $id => $known) {
                if ($known === $combo) {
                    $newId = $id;

                    break;
                }
            }

            if ($newId === null) {
                $newId = count($table);
                $table[] = $combo;
            }

            $map[$oldId] = $newId;
        }

        return $map;
    }

    /**
     * @throws FloeException
     *
     * @return array{footer: Footer, footerFrameStart: int}
     */
    private function readLayout(Path $source): array
    {
        if ($this->filesystem->status($source) === null) {
            throw new FloeException(sprintf('Floe merge source "%s" does not exist', $source->uri()));
        }

        $stream = $this->filesystem->readFrom($source);
        $location = (new FooterReader())->read($stream, $this->codec);
        $stream->close();

        return [
            'footer' => $location->footer,
            'footerFrameStart' => $location->footerFrameStart,
        ];
    }
}
