<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\{Adapter\JSON\RowsNormalizer\EntryNormalizer, FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\{Closure, FileLoader};
use Flow\Filesystem\{DestinationStream, Partition, Path};

final class JsonLinesLoader implements Closure, FileLoader, Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    private int $flags = JSON_THROW_ON_ERROR;

    public function __construct(private readonly Path $path)
    {
    }

    public function closure(FlowContext $context) : void
    {
        $context->streams()->closeStreams($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($rows->partitions()->count()) {
            $this->write($rows, $rows->partitions()->toArray(), $context);
        } else {
            $this->write($rows, [], $context);
        }
    }

    public function withDateTimeFormat(string $dateTimeFormat) : self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withFlags(int $flags) : self
    {
        $this->flags = $flags &= ~JSON_PRETTY_PRINT;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context) : void
    {
        $streams = $context->streams();
        $normalizer = new RowsNormalizer(new EntryNormalizer($this->dateTimeFormat));

        $stream = $streams->writeTo($this->path, $partitions);

        $this->writeJSON($nextRows, $stream, $normalizer);
    }

    /**
     * @param Rows $rows
     * @param DestinationStream $stream
     *
     * @throws RuntimeException
     * @throws \JsonException
     */
    private function writeJSON(Rows $rows, DestinationStream $stream, RowsNormalizer $normalizer) : void
    {
        if (!\count($rows)) {
            return;
        }

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            try {
                $json = json_encode($normalizedRow, $this->flags);

                if ($json === false) {
                    throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
                }
            } catch (\JsonException $e) {
                throw new RuntimeException('Failed to encode JSON: ' . $e->getMessage(), 0, $e);
            }

            $stream->append($json . "\n");
        }
    }
}
