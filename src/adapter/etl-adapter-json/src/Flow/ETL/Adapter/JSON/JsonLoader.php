<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\Closure;
use Flow\ETL\{Adapter\JSON\RowsNormalizer\EntryNormalizer, FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Partition, Path};

final class JsonLoader implements Closure, Loader, Loader\FileLoader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    private int $flags = JSON_THROW_ON_ERROR;

    private bool $putRowsInNewLines = false;

    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(private readonly Path $path)
    {
    }

    public function closure(FlowContext $context) : void
    {
        foreach ($context->streams() as $stream) {
            if ($stream->path()->extension() === 'json') {
                $stream->append($this->putRowsInNewLines ? "\n]" : ']');
            }
        }

        $context->streams()->closeWriters($this->path);
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
        $this->flags = $flags;

        return $this;
    }

    public function withRowsInNewLines(bool $putRowsInNewLines) : self
    {
        $this->putRowsInNewLines = $putRowsInNewLines;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context) : void
    {
        $streams = $context->streams();
        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        if (!$streams->isOpen($this->path, $partitions)) {
            $stream = $streams->writeTo($this->path, $partitions);

            if (!\array_key_exists($stream->path()->path(), $this->writes)) {
                $this->writes[$stream->path()->path()] = 0;
            }

            $stream->append($this->putRowsInNewLines ? "[\n" : '[');
        } else {
            $stream = $streams->writeTo($this->path, $partitions);
        }

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

        $separator = $this->putRowsInNewLines ? ",\n" : ',';

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            try {
                $json = json_encode($normalizedRow, $this->flags);

                if ($json === false) {
                    throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
                }
            } catch (\JsonException $e) {
                throw new RuntimeException('Failed to encode JSON: ' . $e->getMessage(), 0, $e);
            }

            $json = ($this->writes[$stream->path()->path()] > 0) ? ($separator . $json) : $json;

            $stream->append($json);

            $this->writes[$stream->path()->path()]++;
        }
    }
}
