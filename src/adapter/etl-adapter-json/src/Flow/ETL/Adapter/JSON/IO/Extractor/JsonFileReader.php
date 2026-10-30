<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\SourceStream;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;
use LimitIterator;
use SensitiveParameter;

use function constant;
use function count;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function get_debug_type;
use function is_array;
use function iterator_to_array;
use function json_decode;
use function sprintf;
use function strlen;
use function trim;

/**
 * The read of one listed file, for both the sample and the real read.
 */
final readonly class JsonFileReader implements Describable
{
    private const int CHUNK = 8 * 1024;

    private const string C_ISSPACE = " \t\n\r\v\f";

    /**
     * @var array{decoder: ExtJsonDecoder, pointer?: string}
     */
    private array $options;

    public function __construct(
        private Filesystem $filesystem,
        public JsonFraming $format,
        private ?string $pointer,
        private bool $pointerToColumnName,
    ) {
        $this->options = $pointer === null
            ? ['decoder' => new ExtJsonDecoder(true)]
            : ['decoder' => new ExtJsonDecoder(true), 'pointer' => $pointer];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            type_enum(JsonFraming::class)->assert(constant(
                JsonFraming::class . '::' . type_string()->assert($data['format'] ?? null),
            )),
            type_optional(type_string())->assert($data['pointer'] ?? null),
            type_boolean()->assert($data['pointer_to_column_name'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(
            self::class,
            [
                'filesystem' => new ConfiguredFilesystem($this->filesystem),
                'format' => $this->format->name,
                'pointer' => $this->pointer,
                'pointer_to_column_name' => $this->pointerToColumnName,
            ],
            extensions: ['json'],
        );
    }

    public function pointsInto(): bool
    {
        return $this->pointer !== null;
    }

    /**
     * $split when it narrows the read: Lines only, and not the one split that covers a whole file.
     */
    public function window(?FileSplit $split): ?FileSplit
    {
        return $split !== null
        && $this->format === JsonFraming::Lines
        && !($split->isFirst() && $split->length === null)
            ? $split
            : null;
    }

    /**
     * $first must be exactly $stream->read(self::CHUNK, 0) - the loop resumes at offset self::CHUNK.
     *
     * @return Generator<int, string>
     */
    public function chunks(SourceStream $stream, string $first): Generator
    {
        yield $first;

        for ($offset = self::CHUNK; ($chunk = $stream->read(self::CHUNK, $offset)) !== ''; $offset += self::CHUNK) {
            yield $chunk;
        }
    }

    /**
     * A 0-byte stream is no JSON text and yields nothing
     *
     * A member is `mixed`, not an array: a document whose elements are scalars decodes to scalars, which sample()
     * refuses.
     *
     * @return Generator<mixed, mixed>
     */
    public function documentItems(SourceStream $stream): Generator
    {
        $first = $stream->read(self::CHUNK, 0);

        if ($first === '') {
            return;
        }

        if ($this->pointer !== null) {
            yield from (new Items($this->chunks($stream, $first), $this->options))->getIterator();

            return;
        }

        yield from (new JsonArrayElements())->of(
            $stream,
            $first,
            fn(int $skip): LimitIterator => new LimitIterator(
                (new Items($this->chunks($stream, $first), $this->options))->getIterator(),
                $skip,
            ),
        );
    }

    /**
     * One line at a time, never re-split or re-joined, so a future byte-range unit stays alignable.
     *
     * @return Generator<mixed, mixed>
     */
    public function lineItems(SourceStream $stream, JsonReadBytes $read = new JsonReadBytes()): Generator
    {
        foreach ($stream->readLines() as $line) {
            // readLines() strips the "\n" and hides whether the last line had one, so that line counts 1 byte more
            $read->add(strlen($line) + 1);

            if (trim($line, self::C_ISSPACE) === '') {
                continue;
            }

            if ($this->pointer === null) {
                // json_decode is an order of magnitude faster and agrees with JSON Machine on every object or
                // array line; a line it rejects (a BOM, two objects, a bare scalar) keeps JSON Machine's verdict
                // @mago-ignore analysis:mixed-assignment
                $record = json_decode($line, true);

                yield is_array($record) ? $record : iterator_to_array(Items::fromString($line, $this->options));

                continue;
            }

            yield from Items::fromString($line, $this->options)->getIterator();
        }
    }

    /**
     * The pointer wrap and the empty-record skip live here and nowhere else, so the sample and the read see the
     * same rows.
     *
     * @param null|FileSplit $window the lines whose first byte lies in it, Lines only; null reads the file whole
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public function sample(
        SourceFile $source,
        JsonReadBytes $read = new JsonReadBytes(),
        ?FileSplit $window = null,
    ): Generator {
        $stream = $this->filesystem->readFrom($source->path);

        try {
            if ($window !== null && $this->format === JsonFraming::Lines) {
                $resync = new JsonLineResync();
                $stream = $window->window(
                    $stream,
                    $resync->after($stream, $window->start),
                    $window->length === null ? null : $resync->after($stream, $window->start + $window->length),
                );
            }

            $items = match ($this->format) {
                JsonFraming::Array, JsonFraming::ArrayLines => $this->documentItems($stream),
                JsonFraming::Lines => $this->lineItems($stream, $read),
            };

            // @mago-ignore analysis:mixed-assignment
            foreach ($items as $row) {
                if ($this->pointer !== null && $this->pointerToColumnName) {
                    $row = [$this->pointer => $row];
                }

                if (!is_array($row)) {
                    throw new RuntimeException(sprintf(
                        'A JSON record must be an object or an array, %s given in "%s".',
                        get_debug_type($row),
                        $source->uri(),
                    ));
                }

                // {} decodes to []: a record with no fields is skipped - bug b80
                if (!count($row)) {
                    continue;
                }

                yield $row;
            }
        } finally {
            $stream->close();
        }
    }
}
