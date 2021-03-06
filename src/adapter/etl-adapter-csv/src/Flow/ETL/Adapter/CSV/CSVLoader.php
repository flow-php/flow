<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Partition;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *     path: Path,
 *     header: boolean,
 *     safe_mode: boolean,
 *     separator: string,
 *     enclosure: string,
 *     escape: string,
 *     new_line_separator: string
 *  }>
 */
final class CSVLoader implements Closure, Loader
{
    private ?FilesystemStreams $streams;

    public function __construct(
        private readonly Path $path,
        private bool $header = true,
        private readonly bool $safeMode = false,
        private string $separator = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
        private string $newLineSeparator = PHP_EOL
    ) {
        $this->streams = null;
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'header' => $this->header,
            'safe_mode' => $this->safeMode,
            'separator' => $this->separator,
            'enclosure' => $this->enclosure,
            'escape' => $this->escape,
            'new_line_separator' => $this->newLineSeparator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->header = $data['header'];
        $this->safeMode = $data['safe_mode'];
        $this->separator = $data['separator'];
        $this->escape = $data['escape'];
        $this->enclosure = $data['enclosure'];
        $this->newLineSeparator = $data['new_line_separator'];
        $this->streams = null;
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows, FlowContext $context) : void
    {
        if ($this->streams !== null) {
            $this->streams->close();
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $headers = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());

        if (\count($context->partitionEntries())) {
            foreach ($rows->partitionBy(...$context->partitionEntries()) as $partition) {
                $this->write($partition->rows, $headers, $context, $partition->partitions);
            }
        } else {
            $this->write($rows, $headers, $context, []);
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $headers, FlowContext $context, array $partitions) : void
    {
        $mode = Mode::WRITE;
        $streams = $this->streams($context);

        if ($context->mode() === SaveMode::ExceptionIfExists && $streams->exists($this->path, $partitions)) {
            throw new RuntimeException('Destination path "' . $this->path->uri() . '" already exists, please change path to different or set different SaveMode');
        }

        if ($context->mode() === SaveMode::Ignore && $streams->exists($this->path, $partitions) && !$streams->isOpen($this->path)) {
            return;
        }

        if ($context->mode() === SaveMode::Overwrite && $streams->exists($this->path, $partitions) && !$streams->isOpen($this->path, $partitions)) {
            $streams->rm($this->path, $partitions);
        }

        if ($context->mode() === SaveMode::Append && $streams->exists($this->path, $partitions)) {
            $this->header = false;
            $mode = Mode::APPEND;
        }

        if ($this->header && !$streams->exists($this->path, $partitions)) {
            $this->writeCSV(
                $headers,
                $streams->open($this->path, 'csv', $mode, $this->safeMode, $partitions)
            );
        }

        foreach ($nextRows as $row) {

            /**
             * @psalm-suppress MixedArgumentTypeCoercion
             */
            $this->writeCSV(
                $row->toArray(),
                $streams->open($this->path, 'csv', $mode, $this->safeMode, $partitions)
            );
        }
    }

    private function streams(FlowContext $context) : FilesystemStreams
    {
        if ($this->streams === null) {
            $this->streams = new FilesystemStreams($context->fs());
        }

        return $this->streams;
    }

    private function writeCSV(array $row, FileStream $destination) : void
    {
        /**
         * @psalm-suppress TooManyArguments
         * @psalm-suppress InvalidNamedArgument
         * @psalm-suppress MixedArgumentTypeCoercion
         */
        \fputcsv(
            stream: $destination->resource(),
            fields: $row,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator
        );
    }
}
