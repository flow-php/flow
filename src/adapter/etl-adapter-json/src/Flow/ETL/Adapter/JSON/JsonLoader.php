<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\Closure;
use Flow\ETL\{Adapter\JSON\RowsNormalizer\EntryNormalizer, FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Partition, Path};

final class JsonLoader implements Closure, Loader, Loader\FileLoader
{
    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(
        private readonly Path $path,
        private readonly int $flats = JSON_THROW_ON_ERROR,
        private readonly string $dateTimeFormat = \DateTimeInterface::ATOM,
        private readonly bool $putRowsInNewLines = false
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("JsonLoader path can't be pattern, given: " . $this->path->path());
        }
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
    public function writeJSON(Rows $rows, DestinationStream $stream, RowsNormalizer $normalizer) : void
    {
        if (!\count($rows)) {
            return;
        }

        $separator = $this->putRowsInNewLines ? ",\n" : ',';

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $json = json_encode($normalizedRow, $this->flats);

            if ($json === false) {
                throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
            }

            $json = ($this->writes[$stream->path()->path()] > 0) ? ($separator . $json) : $json;

            $stream->append($json);
        }

        $this->writes[$stream->path()->path()]++;
    }
}
