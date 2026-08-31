<?php

declare(strict_types=1);

namespace Flow\Floe;

use Composer\InstalledVersions;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Types\Type\TypeDetector;

use function get_debug_type;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function sprintf;
use function strlen;
use function substr;

final class FloeStreamWriter
{
    /**
     * SCHEMA EVOLUTION: the session schema is fixed for the writer's life, so any batch that adds
     * a column - even an optional one - or turns a not-null column nullable is rejected rather
     * than evolving the schema. Prior art (BigQuery ALLOW_FIELD_ADDITION / ALLOW_FIELD_RELAXATION,
     * Iceberg, Delta mergeSchema) allows both of those, and safe type widening, on append.
     */
    private const string BATCH_MISMATCH = 'Floe write session schema is fixed and this batch does not fit it: %s.';

    /**
     * Sections exist to bound a seek, so they are cut by size. Nothing else breaks one.
     */
    private const int SECTION_MAX_ROWS = 100_000;

    private Metadata $metadata;

    private bool $open = false;

    /**
     * @var array<int, Section>
     */
    private array $sections = [];

    private int $sectionOffset = 0;

    private int $sectionRowCount = 0;

    private bool $sectionOpen = false;

    private Schema $sessionSchema;

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

        $this->frameWriter = new FrameWriter(
            $stream,
            $this->options->codec->id(),
            $this->options->bufferSize,
            startPosition: $fileLength,
        );
        $this->metadata = $footer->metadata->merge($metadata ?? Metadata::empty());
        $this->sections = $footer->sections;

        if ($footer->schema !== []) {
            // A batch carrying the same columns or struct fields in a different order must append,
            // not throw - conform the order instead of loosening the guard below.
            $this->sessionSchema = $this->sessionSchema->conformOrderTo($footer->schema());

            if ($this->sessionSchema->normalize() !== $footer->schema()->normalize()) {
                throw new IncompatibleSchemaException('Floe append schema does not match the existing file schema.');
            }
        }

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

        if ($rows->count() === 0) {
            return;
        }

        $this->openSession();
        $typed = $this->hydrator->dehydrate($rows);
        $this->assertBatchFitsSession($typed);

        if (!$this->sectionOpen || $this->sectionRowCount >= self::SECTION_MAX_ROWS) {
            $this->startSection();
        }

        $this->emitBatch($typed);
    }

    public static function writerVersion(): string
    {
        return InstalledVersions::isInstalled('flow-php/etl')
            ? InstalledVersions::getPrettyVersion('flow-php/etl') ?? 'unknown'
            : 'unknown';
    }

    /**
     * @throws FloeException
     */
    private function openSession(): void
    {
        if ($this->sessionEncoder !== null) {
            return;
        }

        $this->sessionEncoder = $this->engine->encoder($this->sessionSchema);
    }

    /**
     * @param list<\Flow\ETL\Row\TypedRowValues> $typed
     *
     * @throws IncompatibleSchemaException
     */
    private function assertBatchFitsSession(array $typed): void
    {
        $definitions = $this->sessionSchema->definitions();

        foreach ($typed as $index => $rowValues) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($rowValues->values as $name => $value) {
                $definition = $definitions[$name] ?? null;

                if ($definition === null) {
                    throw new IncompatibleSchemaException(sprintf(self::BATCH_MISMATCH, sprintf(
                        'new column "%s"',
                        $name,
                    )));
                }

                if (!$this->options->validateData) {
                    continue;
                }

                if ($value === null) {
                    if (!$definition->isNullable()) {
                        throw new IncompatibleSchemaException(sprintf(self::BATCH_MISMATCH, sprintf(
                            'column "%s" (row %d): could not convert null to %s, column is not nullable',
                            $name,
                            $index,
                            $definition->type()->toString(),
                        )));
                    }

                    continue;
                }

                if (!$definition->type()->isValid($value)) {
                    throw new IncompatibleSchemaException(sprintf(self::BATCH_MISMATCH, sprintf(
                        'column "%s" (row %d): could not convert %s (%s) to %s',
                        $name,
                        $index,
                        self::describeValue($value),
                        (new TypeDetector())
                            ->detectType($value)
                            ->toString(),
                        $definition->type()->toString(),
                    )));
                }
            }
        }
    }

    private static function describeValue(mixed $value): string
    {
        return match (true) {
            is_string($value) => "'" . (strlen($value) > 32 ? substr($value, 0, 32) . '...' : $value) . "'",
            is_bool($value) => $value ? 'true' : 'false',
            is_int($value), is_float($value) => (string) $value,
            default => get_debug_type($value),
        };
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
            $this->sections[] = new Section($this->sectionOffset, $this->sectionRowCount);
            $this->sectionOpen = false;
            $this->sectionRowCount = 0;
        }
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

        $this->sectionOffset = $this->frameWriter()->position();
        $this->sectionOpen = true;
        $this->sectionRowCount = 0;
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
