<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Composer\InstalledVersions;
use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\ETL\Values\Schema\Validator\EvolvingValidator;
use Flow\Filesystem\DestinationStream;

use function array_key_exists;
use function sprintf;

final class FloeStreamWriter
{
    /**
     * A file holds one schema: a batch that adds a column, even an optional one, or turns a not-null column nullable
     * is rejected.
     */
    private const string BATCH_MISMATCH = 'Floe write session schema is fixed and this batch does not fit it: %s.';

    /**
     * Sections bound a seek, so only size cuts one.
     */
    private const int SECTION_MAX_ROWS = 100_000;

    private bool $closed = false;

    private Metadata $metadata;

    /**
     * @var array<int, Section>
     */
    private array $sections = [];

    private int $sectionOffset = 0;

    private int $sectionRowCount = 0;

    private bool $sectionOpen = false;

    private Schema $sessionSchema;

    private int $totalRows = 0;

    private readonly SplittingFrameEncoder $frameEncoder;

    /**
     * @throws FloeException
     */
    private function __construct(
        private readonly FrameWriter $frameWriter,
        Schema $schema,
        private readonly Backend $backend,
        Options $options,
    ) {
        $this->sessionSchema = $schema;
        $this->frameEncoder = new SplittingFrameEncoder($this->backend, $options->codec);
    }

    /**
     * @param Schema $schema fixes the session schema for the writer's life
     *
     * @throws FloeException
     */
    public static function create(
        DestinationStream $stream,
        Schema $schema,
        Backend $backend,
        Options $options = new Options(),
        ?Metadata $metadata = null,
    ): self {
        Format::validateCodecId($options->codec->id());

        $writer = new self(
            new FrameWriter($stream, $options->codec->id(), $options->bufferSize),
            $schema,
            $backend,
            $options,
        );
        $writer->metadata = $metadata ?? Metadata::empty();
        $writer->frameWriter->header();

        return $writer;
    }

    /**
     * @param ?Metadata $metadata merged over the footer metadata, new keys win
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public static function resume(
        DestinationStream $stream,
        Footer $footer,
        int $fileLength,
        Schema $schema,
        Backend $backend,
        Options $options = new Options(),
        ?Metadata $metadata = null,
    ): self {
        Format::validateCodecId($options->codec->id());

        $writer = new self(
            new FrameWriter($stream, $options->codec->id(), $options->bufferSize, startPosition: $fileLength),
            $schema,
            $backend,
            $options,
        );
        $writer->metadata = $footer->metadata->merge($metadata ?? Metadata::empty());
        $writer->sections = $footer->sections;

        if ($footer->schema !== []) {
            // A batch carrying the same columns or struct fields in a different order must append,
            // not throw - match the order instead of loosening the guard below.
            $writer->sessionSchema = $schema->matchOrderTo($footer->schema());

            if ($writer->sessionSchema->normalize() !== $footer->schema()->normalize()) {
                throw new IncompatibleSchemaException('Floe append schema does not match the existing file schema.');
            }
        }

        $writer->totalRows = $footer->statistics->rows;

        return $writer;
    }

    /**
     * @throws FloeException
     */
    public function close(): void
    {
        $this->guardNotClosed();
        $this->closeSection();

        /** @var array<int, array<string, mixed>> $schema */
        $schema = $this->sessionSchema->normalize();

        $this->frameWriter->footer(
            (new Footer(
                Format::VERSION,
                self::writerVersion(),
                $schema,
                $this->sections,
                new FooterStatistics($this->totalRows, $this->frameWriter->position() - Format::HEADER_LENGTH),
                $this->metadata,
            ))->toJson(),
        );
        $this->frameWriter->close();
        $this->closed = true;
    }

    /**
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public function write(Rows $rows): void
    {
        $this->guardNotClosed();

        if ($rows->count() === 0) {
            return;
        }

        $matched = $rows;

        if (!$rows->schema()->isSame($this->sessionSchema)) {
            $this->assertBatchFitsSession($rows->schema());
            $matched = $rows->matchTo($this->sessionSchema, $this->backend);
        }

        $bodies = $this->frameEncoder->encode($matched);

        $this->startSectionWhenDue();

        foreach ($bodies as [$body, $count]) {
            $this->frameWriter->frame(Format::FRAME_BATCH, $body);
            $this->sectionRowCount += $count;
            $this->totalRows += $count;
        }
    }

    public static function writerVersion(): string
    {
        return InstalledVersions::isInstalled('flow-php/etl')
            ? InstalledVersions::getPrettyVersion('flow-php/etl') ?? 'unknown'
            : 'unknown';
    }

    /**
     * @throws IncompatibleSchemaException
     */
    private function assertBatchFitsSession(Schema $batch): void
    {
        $definitions = $this->sessionSchema->definitions();

        // unconditional, and before the validator: EvolvingValidator admits a nullable extra column,
        // which the session schema cannot carry
        foreach ($batch->definitions() as $name => $_) {
            if (!array_key_exists($name, $definitions)) {
                throw new IncompatibleSchemaException(sprintf(self::BATCH_MISMATCH, sprintf('new column "%s"', $name)));
            }
        }

        $validation = (new EvolvingValidator())->validate($this->sessionSchema, $batch);

        if (!$validation->isValid()) {
            throw new IncompatibleSchemaException(sprintf(self::BATCH_MISMATCH, $validation->toString()));
        }
    }

    private function closeSection(): void
    {
        if ($this->sectionOpen) {
            $this->sections[] = new Section($this->sectionOffset, $this->sectionRowCount);
            $this->sectionOpen = false;
            $this->sectionRowCount = 0;
        }
    }

    /**
     * @throws FloeException
     */
    private function guardNotClosed(): void
    {
        if ($this->closed) {
            throw new FloeException('Floe writer session is closed');
        }
    }

    /**
     * @throws FloeException
     */
    private function startSectionWhenDue(): void
    {
        if (!$this->sectionOpen || $this->sectionRowCount >= self::SECTION_MAX_ROWS) {
            $this->startSection();
        }
    }

    /**
     * @throws FloeException
     */
    private function startSection(): void
    {
        $this->closeSection();

        $this->sectionOffset = $this->frameWriter->position();
        $this->sectionOpen = true;
        $this->sectionRowCount = 0;
    }
}
