<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
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
        private readonly bool $header = true,
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

        if ($this->streams === null) {
            $this->streams = new FilesystemStreams($context->fs());
        }

        $streams = $this->streams;

        $headers = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());

        if (\count($context->partitionEntries())) {
            foreach ($rows->partitionBy(...$context->partitionEntries()) as $partitionedRows) {
                if ($this->header && !$streams->exists($this->path, $partitionedRows->partitions)) {
                    $this->append(
                        $headers,
                        $streams->open($this->path, 'csv', Mode::WRITE, $this->safeMode, $partitionedRows->partitions)
                    );
                }

                foreach ($partitionedRows->rows as $row) {
                    /**
                     * @psalm-suppress MixedArgumentTypeCoercion
                     */
                    $this->append(
                        $row->toArray(),
                        $streams->open($this->path, 'csv', Mode::WRITE, $this->safeMode, $partitionedRows->partitions)
                    );
                }
            }
        } else {
            if ($this->header && !$streams->exists($this->path)) {
                $this->append(
                    $headers,
                    $streams->open($this->path, 'csv', Mode::WRITE, $this->safeMode)
                );
            }

            foreach ($rows as $row) {

                /**
                 * @psalm-suppress MixedArgumentTypeCoercion
                 */
                $this->append(
                    $row->toArray(),
                    $streams->open($this->path, 'csv', Mode::WRITE, $this->safeMode)
                );
            }
        }
    }

    private function append(array $row, FileStream $destination) : void
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
