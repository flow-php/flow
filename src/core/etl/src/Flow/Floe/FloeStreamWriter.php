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
     * @var array<int, array<string, string>>
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

    private Schema $sessionSchema;

    private ?string $sessionSchemaBody = null;

    /**
     * @var null|Encoder<string>
     */
    private ?Encoder $sessionEncoder = null;

    private ?FrameWriter $frameWriter = null;

    private int $totalRows = 0;

    private readonly Hydrator $hydrator;

    /**
     * @throws FloeException
     */
    public function __construct(
        Schema $schema,
        private readonly Options $options = new Options(),
        ?Hydrator $hydrator = null,
        private readonly FloeEngine $engine = FloeEngine::adaptive,
    ) {
        Format::validateCodecId($this->options->codec->id());
        $this->sessionSchema = $schema;
        $this->hydrator = $hydrator ?? new AdaptiveRowHydrator();
    }

    /**
     * @throws FloeException
     */
    public function create(DestinationStream $stream, ?Metadata $metadata = null): void
    {
        $this->guardNotOpen();

        $this->frameWriter = new FrameWriter($stream, $this->options->codec->id(), $this->options->bufferSize);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->frameWriter->header();
        $this->open = true;
    }

    /**
     * @throws FloeException
     */
    public function resume(DestinationStream $stream, Footer $footer, int $fileLength, ?Metadata $metadata = null): void
    {
        $this->guardNotOpen();

        $lastSection = $footer->sections === [] ? null : $footer->sections[count($footer->sections) - 1];

        $this->frameWriter = new FrameWriter(
            $stream,
            $this->options->codec->id(),
            $this->options->bufferSize,
            startPosition: $fileLength,
        );
        $this->metadata = $footer->metadata->merge($metadata ?? Metadata::empty());
        $this->sections = $footer->sections;

        if ($footer->schema !== []) {
            if ($this->sessionSchema->normalize() !== $footer->schema()->normalize()) {
                throw new IncompatibleSchemaException(
                    'Floe append schema does not match the existing file schema. '
                    . 'Align the pipeline with DataFrame::match($schema) before appending.',
                );
            }

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
        $schema = $this->sessionSchema->normalize();

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

        $this->openSession();

        if ($this->options->validateData) {
            $this->assertFitsSession(self::unionSchema($rows));
        }

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

    public static function unionSchema(Rows $rows): Schema
    {
        $schema = null;

        foreach ($rows->all() as $row) {
            $rowSchema = self::rowSchema($row);
            $schema = $schema === null ? $rowSchema : $schema->merge($rowSchema);
        }

        return $schema ?? new Schema();
    }

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
    private function openSession(): void
    {
        if ($this->sessionEncoder !== null) {
            return;
        }

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
     * @throws IncompatibleSchemaException
     */
    private function assertFitsSession(Schema $batchSchema): void
    {
        $session = $this->sessionSchema;

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
     * @param list<\Flow\ETL\Row\TypedRowValues> $typed
     *
     * @throws FloeException
     */
    private function emitBatch(array $typed): void
    {
        foreach ($this->sessionEncoder()->encode($typed) as $encoded) {
            $this->frameWriter()->row($this->options->codec->encode($encoded));
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
