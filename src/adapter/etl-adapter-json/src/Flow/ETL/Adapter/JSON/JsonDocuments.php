<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Filesystem\FilesSink;
use Flow\Filesystem\Partition;

/**
 * The per-file write counts live here, not on the sink: the separator decision belongs to the run.
 */
final class JsonDocuments
{
    /** @var array<string, int> */
    private array $writes = [];

    public function __construct(
        private readonly FilesSink $files,
        private readonly bool $putRowsInNewLines,
    ) {}

    /**
     * @param list<string> $documents
     * @param array<Partition> $partitions
     */
    public function append(array $documents, array $partitions): void
    {
        $opening = !$this->files->touched($partitions);
        $stream = $this->files->writeTo($partitions);
        $uri = $stream->path()->uri();

        if ($opening) {
            $stream->append($this->putRowsInNewLines ? "[\n" : '[');
        }

        $this->writes[$uri] ??= 0;
        $separator = $this->putRowsInNewLines ? ",\n" : ',';

        foreach ($documents as $document) {
            $stream->append($this->writes[$uri] > 0 ? $separator . $document : $document);
            $this->writes[$uri]++;
        }
    }

    public function abandon(): void
    {
        $this->files->abandon();
    }

    public function publish(): void
    {
        foreach ($this->files->openStreams() as $stream) {
            $stream->append($this->putRowsInNewLines ? "\n]" : ']');
        }

        $this->files->publish();
    }
}
