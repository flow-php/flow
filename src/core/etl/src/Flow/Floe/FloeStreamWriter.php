<?php

declare(strict_types=1);

namespace Flow\Floe;

use Composer\InstalledVersions;
use Flow\ETL\Row;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Types\Type\Native\NullType;
use JsonException;

use function count;
use function Flow\Types\DSL\type_equals;
use function implode;
use function json_encode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final class FloeStreamWriter
{
    private Metadata $metadata;

    private bool $open = false;

    /**
     * @var array<int, array<string, string>> deduped PARTITIONS combinations, index = partitionsId
     */
    private array $partitions = [];

    private ?int $lastSectionPartitionsId = null;

    private ?int $sectionPartitionsId = null;

    private ?int $pendingPartitionsId = null;

    private bool $forcePartitionBreak = false;

    /**
     * @var array<int, Section>
     */
    private array $sections = [];

    private int $sectionOffset = 0;

    private int $sectionRowCount = 0;

    private bool $sectionOpen = false;

    private bool $schemaFrameWritten = false;

    /**
     * The one schema fixed for the whole session: explicit at create, the file
     * schema on resume, else the first batch's union.
     */
    private ?Schema $sessionSchema = null;

    private ?string $sessionSchemaBody = null;

    /**
     * @var null|Encoder<string> built once from the session schema
     */
    private ?Encoder $sessionEncoder = null;

    private ?FrameWriter $frameWriter = null;

    private int $totalRows = 0;

    private readonly Hydrator $hydrator;

    /**
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     *
     * @throws FloeException
     */
    public function __construct(
        private readonly Codec $codec = new NoopCodec(),
        ?Hydrator $hydrator = null,
        private readonly int $bufferSize = 65_536,
        private readonly FloeEngine $engine = FloeEngine::adaptive,
    ) {
        Format::validateCodecId($this->codec->id());
        $this->hydrator = $hydrator ?? new AdaptiveRowHydrator();
    }

    /**
     * Opens a create session over a fresh destination stream, writing the header.
     *
     * @param ?Metadata $metadata stored in the footer
     * @param ?Schema $schema fixes the session schema; null derives it from the first batch
     *
     * @throws FloeException
     */
    public function create(DestinationStream $stream, ?Metadata $metadata = null, ?Schema $schema = null): void
    {
        $this->guardNotOpen();

        $this->frameWriter = new FrameWriter($stream, $this->codec->id(), $this->bufferSize);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->sessionSchema = $schema;
        $this->frameWriter->header();
        $this->open = true;
    }

    /**
     * Seeds writer state from an existing file's footer and opens an append
     * session over its stream. The caller (facade) owns the filesystem-level
     * validation (header/trailer/torn/codec checks) before handing over. The
     * session schema is forced to the file union - appended batches must fit it.
     *
     * @param ?Metadata $metadata merged over the existing footer metadata, new keys win
     *
     * @throws FloeException
     */
    public function resume(DestinationStream $stream, Footer $footer, int $fileLength, ?Metadata $metadata = null): void
    {
        $this->guardNotOpen();

        $lastSection = $footer->sections === [] ? null : $footer->sections[count($footer->sections) - 1];

        $this->frameWriter = new FrameWriter(
            $stream,
            $this->codec->id(),
            $this->bufferSize,
            startPosition: $fileLength,
        );
        $this->metadata = $footer->metadata->merge($metadata ?? Metadata::empty());
        $this->sections = $footer->sections;

        // a zero-row file records no schema - its session is still schemaless and the
        // first appended batch fixes the schema, exactly as on create
        if ($footer->schema !== []) {
            $this->sessionSchema = $footer->schema();
            $this->sessionSchemaBody = $footer->schemaBody();
        }

        $this->schemaFrameWritten = $footer->sections !== [];
        $this->lastSectionPartitionsId = $lastSection?->partitionsId;
        $this->partitions = $footer->partitions;
        $this->totalRows = $footer->totalRows;
        $this->open = true;
    }

    /**
     * @throws FloeException
     */
    public function close(): void
    {
        $this->guardOpen();

        $this->closeSection();

        /** @var array<int, array<string, mixed>> $schema */
        $schema = $this->sessionSchema?->normalize() ?? [];

        $footerJson = (new Footer(
            Format::VERSION,
            self::writerVersion(),
            $schema,
            $this->sections,
            $this->partitions,
            $this->totalRows,
            $this->metadata,
        ))->toJson();

        $this->frameWriter()->footer($footerJson);
        $this->frameWriter()->close();
        $this->open = false;
    }

    /**
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public function write(Rows $rows): void
    {
        $this->guardOpen();
        $this->trackPartitions($rows);

        if ($rows->count() === 0) {
            return;
        }

        $batchSchema = self::unionSchema($rows);

        if ($this->sessionEncoder === null) {
            $this->openSession($batchSchema);
        }

        $this->assertFitsSession($batchSchema);

        if (!$this->sectionOpen || $this->forcePartitionBreak) {
            $this->startSection();
        }

        $this->emitBatch($this->hydrator->dehydrate($rows));
    }

    public static function writerVersion(): string
    {
        return InstalledVersions::isInstalled('flow-php/etl')
            ? InstalledVersions::getPrettyVersion('flow-php/etl') ?? 'unknown'
            : 'unknown';
    }

    /**
     * The union schema of a batch, built without touching the rows' lazily-cached
     * schema() - the writer must not mutate caller rows.
     */
    public static function unionSchema(Rows $rows): Schema
    {
        $schema = null;

        foreach ($rows->all() as $row) {
            $rowSchema = self::rowSchema($row);
            $schema = $schema === null ? $rowSchema : $schema->merge($rowSchema);
        }

        return $schema ?? new Schema();
    }

    /**
     * The row's schema built from its entry definitions, without touching the
     * row's lazily-cached schema().
     */
    private static function rowSchema(Row $row): Schema
    {
        $definitions = [];

        foreach ($row->entries()->all() as $entry) {
            $definitions[] = $entry->definition();
        }

        return new Schema(...$definitions);
    }

    /**
     * @throws FloeException
     */
    private function openSession(Schema $batchSchema): void
    {
        $this->sessionSchema ??= $batchSchema;
        $this->sessionSchemaBody ??= self::encodeSchemaBody($this->sessionSchema);
        $this->sessionEncoder = $this->engine->encoder($this->sessionSchema);
    }

    /**
     * @throws FloeException
     */
    private static function encodeSchemaBody(Schema $schema): string
    {
        try {
            return json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to encode schema as JSON: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * A batch fits the session when every one of its columns exists in the
     * session schema with a type the session column accepts (nullable-narrower
     * is fine); a wholly-null column fits any column. A new column or an
     * incompatible type throws, naming the offending column(s).
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    private function assertFitsSession(Schema $batchSchema): void
    {
        $session = $this->sessionSchema ?? throw new FloeException('Floe writer has no active session schema');

        $violations = [];

        foreach ($batchSchema->definitions() as $batchDefinition) {
            $name = $batchDefinition->entry()->name();
            $sessionDefinition = $session->findDefinition($batchDefinition->entry());

            if ($sessionDefinition === null) {
                $violations[] = sprintf('new column "%s"', $name);

                continue;
            }

            if ($batchDefinition->type() instanceof NullType) {
                continue;
            }

            // nullable-narrower is fine (a batch value fits a wider session column); nulls into a
            // non-null column or a changed concrete type are drift. Compared structurally via
            // type_equals - Definition::isCompatible reconstructs container element definitions,
            // which throws for element types without a Definition class (mixed, timezone, ...).
            if (
                !$sessionDefinition->isNullable() && $batchDefinition->isNullable()
                || !type_equals($sessionDefinition->type(), $batchDefinition->type())
            ) {
                $violations[] = sprintf(
                    'column "%s" (%s) is not compatible with the session type (%s)',
                    $name,
                    $batchDefinition->type()->toString(),
                    $sessionDefinition->type()->toString(),
                );
            }
        }

        if ($violations !== []) {
            throw new IncompatibleSchemaException(sprintf('Floe write session schema is fixed and this batch does not fit it: %s. '
            . 'Align the pipeline with DataFrame::match($schema) before writing.', implode('; ', $violations)));
        }
    }

    /**
     * Body-encodes the whole batch in one call, then applies the codec and frames
     * each row body.
     *
     * @param list<\Flow\ETL\Row\TypedRowValues> $typed
     *
     * @throws FloeException
     */
    private function emitBatch(array $typed): void
    {
        foreach ($this->sessionEncoder()->encode($typed) as $encoded) {
            $this->frameWriter()->row($this->codec->encode($encoded));
            $this->sectionRowCount++;
            $this->totalRows++;
        }
    }

    private function closeSection(): void
    {
        if ($this->sectionOpen) {
            $this->sections[] = new Section(
                $this->sectionOffset,
                $this->sectionPartitionsId ?? throw new FloeException(
                    'Floe writer has no active section partitions id',
                ),
                $this->sectionRowCount,
            );
            $this->lastSectionPartitionsId = $this->sectionPartitionsId;
            $this->sectionOpen = false;
            $this->sectionRowCount = 0;
        }
    }

    /**
     * Records the batch's partition combination (in the caller's original order,
     * no ksort) into the deduped table and flags a forced section break when it
     * differs from the currently open section - a partition change starts a new
     * section under the same session schema.
     */
    private function trackPartitions(Rows $rows): void
    {
        $combo = [];

        foreach ($rows->partitions() as $partition) {
            $combo[$partition->name] = $partition->value;
        }

        $partitionsId = null;

        foreach ($this->partitions as $id => $known) {
            if ($known === $combo) {
                $partitionsId = $id;

                break;
            }
        }

        if ($partitionsId === null) {
            $partitionsId = count($this->partitions);
            $this->partitions[] = $combo;
        }

        $this->pendingPartitionsId = $partitionsId;
        $this->forcePartitionBreak = $partitionsId !== $this->sectionPartitionsId;
    }

    /**
     * @throws FloeException
     */
    private function guardNotOpen(): void
    {
        if ($this->frameWriter !== null) {
            throw new FloeException('Floe writer session is already open');
        }
    }

    /**
     * @throws FloeException
     */
    private function guardOpen(): void
    {
        if (!$this->open) {
            throw new FloeException('Floe writer session is not open');
        }
    }

    /**
     * Opens a new section over the fixed session schema. The single SCHEMA frame
     * is written once, before the first section; a PARTITIONS frame only when the
     * combination changes.
     *
     * @throws FloeException
     */
    private function startSection(): void
    {
        $this->closeSection();

        $partitionsId = $this->pendingPartitionsId ?? throw new FloeException(
            'Floe writer starting a section before its partitions were tracked',
        );

        $this->sectionOffset = $this->frameWriter()->position();

        if ($this->partitionsFrameChanged($partitionsId)) {
            $this->frameWriter()->partitions($this->partitions[$partitionsId]);
        }

        if (!$this->schemaFrameWritten) {
            $this->frameWriter()->schema(
                $this->sessionSchemaBody ?? throw new FloeException('Floe writer has no session schema body'),
            );
            $this->schemaFrameWritten = true;
        }

        $this->sectionOpen = true;
        $this->sectionPartitionsId = $partitionsId;
        $this->sectionRowCount = 0;
        $this->forcePartitionBreak = false;
    }

    /**
     * A PARTITIONS frame is written only when the section's combination differs
     * from what the reader currently holds - mirror of the SCHEMA-frame dedup.
     * The reader starts at the empty combination, so the first section emits a
     * frame only when it is partitioned; a later change back to unpartitioned
     * emits an empty-body (count=0) frame.
     */
    private function partitionsFrameChanged(int $partitionsId): bool
    {
        if ($this->lastSectionPartitionsId === null) {
            return $this->partitions[$partitionsId] !== [];
        }

        return $partitionsId !== $this->lastSectionPartitionsId;
    }

    /**
     * @throws FloeException
     *
     * @return Encoder<string>
     */
    private function sessionEncoder(): Encoder
    {
        return $this->sessionEncoder ?? throw new FloeException('Floe writer has no active session encoder');
    }

    /**
     * @throws FloeException
     */
    private function frameWriter(): FrameWriter
    {
        return $this->frameWriter ?? throw new FloeException('Floe writer session is not open');
    }
}
