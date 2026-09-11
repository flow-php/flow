<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\SourceStream;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;
use LimitIterator;

use function count;
use function is_array;
use function iterator_to_array;
use function json_decode;
use function trim;

/**
 * The read of one listed file, for both the sample and the real read.
 */
final readonly class JsonFileReader implements SchemaSampler
{
    private const int CHUNK = 8 * 1024;

    private const string C_ISSPACE = " \t\n\r\v\f";

    /**
     * @var array{decoder: ExtJsonDecoder, pointer?: string}
     */
    private array $options;

    /**
     * @param list<SourceFile> $sources the extractor's materialised listing, already filtered, so filesToSniff
     *                                  counts filtered files; samples() and the extractor's own loop both walk
     *                                  it, which a Generator could not serve twice
     */
    public function __construct(
        private Filesystem $filesystem,
        private JsonFormat $format,
        private ?string $pointer,
        private bool $pointerToEntryName,
        private array $sources,
    ) {
        $this->options = $pointer === null
            ? ['decoder' => new ExtJsonDecoder(true)]
            : ['decoder' => new ExtJsonDecoder(true), 'pointer' => $pointer];
    }

    /**
     * Abandoning the generator closes the source.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, non-empty-list<RawRowValues>>
     */
    public function batches(SourceFile $source, int $batchSize): Generator
    {
        $batch = [];

        foreach ($this->sample($source) as $values) {
            $batch[] = $values;

            if (count($batch) >= $batchSize) {
                yield $batch;
                $batch = [];
            }
        }

        if ($batch !== []) {
            yield $batch;
        }
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
     * A member is `mixed`, not an array: a document whose elements are scalars decodes to scalars, which reach
     * count() in sample() and raise a TypeError - bug b79.
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
    public function lineItems(SourceStream $stream): Generator
    {
        foreach ($stream->readLines() as $line) {
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
     * @return Generator<int, RawRowValues>
     */
    public function sample(SourceFile $source): Generator
    {
        $stream = $this->filesystem->readFrom($source->path);

        try {
            $items = match ($this->format) {
                JsonFormat::Document => $this->documentItems($stream),
                JsonFormat::Lines => $this->lineItems($stream),
            };

            /** @var array<string, mixed> $row */
            foreach ($items as $row) {
                if ($this->pointer !== null && $this->pointerToEntryName) {
                    $row = [$this->pointer => $row];
                }

                // {} decodes to []: a record with no fields is skipped - bug b80
                if (!count($row)) {
                    continue;
                }

                yield new RawRowValues($row);
            }
        } finally {
            $stream->close();
        }
    }

    /**
     * $rowBudget is deliberately unused: sample() is lazy and SchemaInferrer stops advancing it.
     *
     * @return Generator<int, Generator<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable
    {
        foreach ($this->sources as $source) {
            yield $this->sample($source);
        }
    }
}
