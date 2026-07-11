<?php

declare(strict_types=1);

namespace Flow\Floe;

use Composer\InstalledVersions;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;

use function array_key_exists;
use function array_values;
use function count;
use function extension_loaded;
use function json_decode;
use function json_encode;
use function ksort;
use function pack;
use function sprintf;
use function strlen;

use const JSON_THROW_ON_ERROR;

final class FloeWriter
{
    private const int IO_BUFFER_SIZE = 65_536;

    private string $buffer = '';

    private readonly RowFrameEncoder $encoder;

    /**
     * Logical write position: flushed bytes + buffered bytes.
     */
    private int $length = 0;

    private Metadata $metadata;

    private ?Schema $mergedSchema = null;

    private bool $open = false;

    /**
     * @var null|array<string, string> null until the first write fixes the file's partitions
     */
    private ?array $partitions = null;

    private ?Schema $appendBaseSchema = null;

    private ?int $lastSectionSchemaId = null;

    /**
     * @var array<int, array<int, array<string, mixed>>>
     */
    private array $schemas = [];

    /**
     * @var array<int, Section>
     */
    private array $sections = [];

    private int $sectionOffset = 0;

    private int $sectionRowCount = 0;

    private ?int $sectionSchemaId = null;

    private ?DestinationStream $stream = null;

    private int $totalRows = 0;

    /**
     * @param null|bool $useExtension null auto-detects flow_php; even when true, a non-Noop codec gets the PHP engine
     *
     * @throws FloeException
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Codec $codec = new NoopCodec(),
        private readonly ?bool $useExtension = null,
    ) {
        Format::validateCodecId($this->codec->id());
        $this->encoder =
            ($this->useExtension ?? extension_loaded('flow_php')) && $this->codec instanceof NoopCodec
                ? new ExtRowFrameEncoder()
                : new PhpRowFrameEncoder($this->codec);
    }

    /**
     * Opens an append session on an existing file; a missing or empty file is
     * created instead. Torn files (invalid trailer) throw.
     *
     * @param ?Metadata $metadata merged over the existing footer metadata, new keys win
     *
     * @throws FloeException
     */
    public function append(Path $path, ?Metadata $metadata = null): void
    {
        $this->guardNotOpen();

        if ($this->filesystem->status($path) === null) {
            $this->create($path, $metadata);

            return;
        }

        $source = $this->filesystem->readFrom($path);
        $size = $source->size();

        if ($size === null) {
            $source->close();

            throw new FloeException(sprintf(
                'Floe append requires a sized stream, "%s" does not report its size',
                $path->uri(),
            ));
        }

        if ($size === 0) {
            $source->close();
            $this->create($path, $metadata);

            return;
        }

        if ($size < (Format::HEADER_LENGTH + Format::TRAILER_LENGTH)) {
            $source->close();

            throw new FloeException(sprintf(
                'Floe file "%s" is torn, too small to hold a header and a trailer',
                $path->uri(),
            ));
        }

        $flags = Format::validateHeader($source->read(Format::HEADER_LENGTH, 0));

        if ($flags !== $this->codec->id()) {
            $source->close();

            throw new FloeException(sprintf(
                'Floe file "%s" was written with codec 0x%02X, expected 0x%02X',
                $path->uri(),
                $flags,
                $this->codec->id(),
            ));
        }

        $footerLength = Format::parseTrailer($source->read(Format::TRAILER_LENGTH, $size - Format::TRAILER_LENGTH));

        if (($size - Format::TRAILER_LENGTH - $footerLength) < Format::HEADER_LENGTH) {
            $source->close();

            throw new FloeException(sprintf(
                'Floe file "%s" is torn, footer does not fit inside the file',
                $path->uri(),
            ));
        }

        /** @var int<1, max> $footerLength */
        $footer = Footer::fromJson($source->read($footerLength, $size - Format::TRAILER_LENGTH - $footerLength));
        $source->close();

        $lastSection = $footer->sections === [] ? null : $footer->sections[count($footer->sections) - 1];

        $this->stream = $this->filesystem->appendTo($path);
        $this->metadata = $footer->metadata->merge($metadata ?? Metadata::empty());
        $this->schemas = $footer->schemas;
        $this->sections = $footer->sections;
        $this->appendBaseSchema = $footer->fileSchema();
        $this->lastSectionSchemaId = $lastSection?->schemaId;
        $this->length = $size;
        $this->mergedSchema = $footer->fileSchema();
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

        /** @var array<int, array<string, mixed>> $fileSchema */
        $fileSchema = $this->mergedSchema?->normalize() ?? [];

        $footerJson = (new Footer(
            Format::VERSION,
            self::writerVersion(),
            $this->schemas,
            $fileSchema,
            $this->sections,
            $this->partitions ?? [],
            $this->totalRows,
            $this->metadata,
        ))->toJson();

        $this->buffer .= Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson)));

        $this->flush();
        $this->stream()->close();
        $this->open = false;
    }

    /**
     * Opens a create session, overwriting an existing file.
     *
     * @param ?Metadata $metadata stored in the footer
     *
     * @throws FloeException
     */
    public function create(Path $path, ?Metadata $metadata = null): void
    {
        $this->beginCreate($this->filesystem->writeTo($path), $metadata ?? Metadata::empty());
    }

    /**
     * Opens a create session over an already-open destination stream (e.g. one
     * provided by the ETL FilesystemStreams machinery). Produces byte-identical
     * output to create(); the only difference is who owns the stream.
     *
     * @param ?Metadata $metadata stored in the footer
     *
     * @throws FloeException
     */
    public function createOnStream(DestinationStream $stream, ?Metadata $metadata = null): void
    {
        $this->beginCreate($stream, $metadata ?? Metadata::empty());
    }

    /**
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public function write(Rows $rows): void
    {
        $this->guardOpen();

        $this->fixPartitions($rows);

        foreach ($this->encoder->encode($rows) as $segment) {
            if ($segment->schemaBody !== null) {
                $this->startSectionFromBody($segment->schemaBody);
            }

            $this->buffer .= $segment->frames;
            $this->length += strlen($segment->frames);
            $this->sectionRowCount += $segment->rowCount;
            $this->totalRows += $segment->rowCount;

            if (strlen($this->buffer) >= self::IO_BUFFER_SIZE) {
                $this->flush();
            }
        }
    }

    public static function writerVersion(): string
    {
        return InstalledVersions::isInstalled('flow-php/etl')
            ? InstalledVersions::getPrettyVersion('flow-php/etl') ?? 'unknown'
            : 'unknown';
    }

    public static function growSectionPlan(?string $currentSchemaBody, Row $row): EncoderPlan
    {
        $rowSchema = self::rowSchema($row);

        if ($currentSchemaBody === null) {
            $schema = $rowSchema;
        } else {
            /** @var array<int, array<string, mixed>> $decoded */
            $decoded = json_decode($currentSchemaBody, true, 512, JSON_THROW_ON_ERROR);
            $schema = self::growUnion(Schema::fromArray($decoded), $rowSchema);
        }

        return (new SchemaEncoder(new ValueEncoder()))->encodeSchema($schema);
    }

    /**
     * @throws FloeException
     */
    private function beginCreate(DestinationStream $stream, Metadata $metadata): void
    {
        $this->guardNotOpen();

        $this->stream = $stream;
        $this->metadata = $metadata;
        $this->length = Format::HEADER_LENGTH;
        $this->buffer = Format::header($this->codec->id());
        $this->open = true;
    }

    private function closeSection(): void
    {
        if ($this->sectionSchemaId !== null) {
            $this->sections[] = new Section($this->sectionOffset, $this->sectionSchemaId, $this->sectionRowCount);
            $this->lastSectionSchemaId = $this->sectionSchemaId;
            $this->sectionSchemaId = null;
            $this->sectionRowCount = 0;
        }
    }

    /**
     * @throws FloeException
     */
    private function fixPartitions(Rows $rows): void
    {
        $incoming = [];

        foreach ($rows->partitions() as $partition) {
            $incoming[$partition->name] = $partition->value;
        }

        ksort($incoming);

        if ($this->partitions === null) {
            $this->partitions = $incoming;

            if ($incoming !== []) {
                $body = pack('V', count($incoming));

                foreach ($rows->partitions() as $partition) {
                    $body .=
                        pack('V', strlen($partition->name))
                        . $partition->name
                        . pack('V', strlen($partition->value))
                        . $partition->value;
                }

                $this->buffer .= Format::frame(Format::FRAME_PARTITIONS, $body);
                $this->length += Format::FRAME_HEADER_LENGTH + strlen($body);
            }

            return;
        }

        if ($incoming !== $this->partitions) {
            throw new FloeException(sprintf(
                'Floe file holds one partition combination, got rows partitioned by "%s" into a file partitioned by "%s"',
                json_encode($incoming, JSON_THROW_ON_ERROR),
                json_encode($this->partitions, JSON_THROW_ON_ERROR),
            ));
        }
    }

    private function flush(): void
    {
        if ($this->buffer !== '') {
            $this->stream()->append($this->buffer);
            $this->buffer = '';
        }
    }

    /**
     * @throws FloeException
     */
    private function guardNotOpen(): void
    {
        if ($this->stream !== null) {
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
     * Bookkeeping half of a section change - the engine already grew the union into $schemaBody.
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    private function startSectionFromBody(string $schemaBody): void
    {
        /** @var array<int, array<string, mixed>> $decoded */
        $decoded = json_decode($schemaBody, true, 512, JSON_THROW_ON_ERROR);
        $schema = Schema::fromArray($decoded);

        if ($this->appendBaseSchema !== null) {
            (new SchemaEvolution())->validate($this->appendBaseSchema, $schema);
        }

        $this->closeSection();

        $schemaId = null;

        foreach ($this->schemas as $id => $known) {
            if ($known == $decoded) {
                $schemaId = $id;

                break;
            }
        }

        if ($schemaId === null) {
            $schemaId = count($this->schemas);
            $this->schemas[] = $decoded;
        }

        $this->sectionOffset = $this->length;

        if ($schemaId !== $this->lastSectionSchemaId) {
            $this->buffer .= Format::frame(Format::FRAME_SCHEMA, $schemaBody);
            $this->length += Format::FRAME_HEADER_LENGTH + strlen($schemaBody);
        }

        $this->mergedSchema = $this->mergedSchema === null ? $schema : $this->mergedSchema->merge($schema);
        $this->sectionSchemaId = $schemaId;
        $this->sectionRowCount = 0;
    }

    /**
     * The row's schema built from its entry definitions, without touching the
     * row's lazily-cached schema() (the writer must not mutate caller rows).
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
     * Unlike Schema::merge, never nullables a column absent from one side - in
     * Floe absence is carried by the row's absent marker, not by a null.
     */
    private static function growUnion(Schema $current, Schema $incoming): Schema
    {
        $definitions = [];

        foreach (array_values($current->definitions()) as $definition) {
            $definitions[$definition->entry()->name()] = $definition;
        }

        foreach (array_values($incoming->definitions()) as $definition) {
            $name = $definition->entry()->name();
            $definitions[$name] = array_key_exists($name, $definitions)
                ? $definitions[$name]->merge($definition)
                : $definition;
        }

        return new Schema(...array_values($definitions));
    }

    /**
     * @throws FloeException
     */
    private function stream(): DestinationStream
    {
        return $this->stream ?? throw new FloeException('Floe writer session is not open');
    }
}
