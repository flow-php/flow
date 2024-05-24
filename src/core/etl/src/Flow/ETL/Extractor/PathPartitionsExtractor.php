<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\{array_entry, row, rows, string_entry};
use Flow\ETL\Filesystem\Path;
use Flow\ETL\{Extractor, FlowContext, Partition};

final class PathPartitionsExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionExtractor
{
    use Limitable;
    use PartitionFiltering;

    public function __construct(private readonly Path $path)
    {

    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($context->config->filesystemStreams()->scan($this->path, $this->partitionFilter()) as $stream) {
            $partitions = $stream->path()->partitions();

            $row = row(
                string_entry('path', $stream->path()->uri()),
                array_entry('partitions', \array_merge(...\array_values(\array_map(static fn (Partition $p) => [$p->name => $p->value], $partitions->toArray()))))
            );

            $signal = yield rows($row);

            $this->countRow();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                $context->streams()->closeWriters($this->path);

                return;
            }
        }
    }

    public function source() : Path
    {
        return $this->path;
    }
}
