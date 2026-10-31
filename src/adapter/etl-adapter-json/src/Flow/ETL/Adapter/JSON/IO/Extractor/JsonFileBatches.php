<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileBatches;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Generator;
use SensitiveParameter;
use Throwable;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function str_starts_with;
use function strlen;
use function strspn;
use function substr;

final readonly class JsonFileBatches implements FileBatches
{
    /**
     * flow_php's `source::CHUNK` (src/extension/flow-php-ext/src/source.rs): the peeked chunk is the first one the
     * Rust source feeds.
     */
    private const int CHUNK = 1 << 15;

    /**
     * @param null|FlowPhpExtension $extension the flow_php extension the Rust source needs; null detects it, never described
     */
    public function __construct(
        private Filesystem $filesystem,
        private JsonFileReader $reader,
        private ?FlowPhpExtension $extension = null,
        private ?int $sampleSize = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            $rebuild->of($data['reader'] ?? null, JsonFileReader::class),
            sampleSize: type_optional(type_integer())->assert($data['sample_size'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
            'reader' => $this->reader,
            'sample_size' => $this->sampleSize,
        ]);
    }

    /**
     * @return Generator<int, CorruptedRecordException|Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $open = $this->open($source, $criteria->split);

        try {
            yield from $open->batches($body, $read->batchSize, $backend, $this->sampleSize, $read->schema);
        } finally {
            $open->close();
        }

        return 0;
    }

    public function close(): void {}

    /**
     * @param null|FileSplit $split the byte window of the one file this read covers; null reads the whole file
     */
    public function open(SourceFile $source, ?FileSplit $split = null): JsonOpenSource
    {
        $window = $this->reader->window($split);

        if (!($this->extension ?? FlowPhpExtension::detect())->available() || $this->reader->pointsInto()) {
            return new PhpJsonOpenSource($this->reader, $source, $window);
        }

        $stream = $this->filesystem->readFrom($source->path);

        try {
            if ($window !== null) {
                $resync = new JsonLineResync();
                $stream = $window->window(
                    $stream,
                    $resync->after($stream, $window->start),
                    $window->length === null ? null : $resync->after($stream, $window->start + $window->length),
                );
            }

            $head = '';
            $offset = 0;

            if ($this->reader->format !== JsonFraming::Lines) {
                // the first byte after JSON whitespace and one leading BOM: '[' is Rust's; an object, a scalar or an
                // empty file is the PHP source's (member iteration / JSON Machine's verdict)
                $first = '';

                for (; ($head = $stream->read(self::CHUNK, $offset)) !== ''; $offset += strlen($head)) {
                    $bytes = $offset === 0 && str_starts_with($head, "\u{FEFF}") ? substr($head, 3) : $head;
                    $skip = strspn($bytes, " \t\n\r");

                    if ($skip < strlen($bytes)) {
                        $first = $bytes[$skip];

                        break;
                    }
                }

                if ($first !== '[') {
                    $stream->close();

                    return new PhpJsonOpenSource($this->reader, $source);
                }
            }

            return new RustJsonOpenSource(
                $stream,
                $this->reader->format === JsonFraming::Lines,
                $source->uri(),
                $head,
                $offset,
            );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }
}
