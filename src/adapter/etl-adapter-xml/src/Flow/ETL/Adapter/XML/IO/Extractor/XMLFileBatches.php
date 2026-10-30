<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Extractor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileBatches;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\XMLDefinition;
use Generator;
use SensitiveParameter;

use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function max;

final readonly class XMLFileBatches implements FileBatches
{
    /**
     * @param int<1, max> $bufferSize
     */
    public function __construct(
        private string $xmlNodePath,
        private int $bufferSize,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['node_path'] ?? null),
            max(1, type_integer()->assert($data['buffer_size'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(
            self::class,
            [
                'node_path' => $this->xmlNodePath,
                'buffer_size' => $this->bufferSize,
            ],
            extensions: ['dom', 'xml', 'xmlreader', 'xmlwriter'],
        );
    }

    /**
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $batches = new XMLNodeBatches(new XMLNodes($this->xmlNodePath), $read->batchSize, $this->bufferSize);
        $node = $body->get('node');
        $stream = $read->files->filesystem->readFrom($source->path);

        try {
            yield from match (true) {
                $node instanceof XMLDefinition && count($body->definitions()) === 1 => $batches->physicals(
                    $stream,
                    $node,
                    $body,
                    $backend,
                ),
                $node instanceof StringDefinition => $batches->strings($stream, $body, $backend),
                default => $batches->documents($stream, $body, $backend),
            };
        } finally {
            $stream->close();
        }

        return 0;
    }

    public function close(): void {}
}
