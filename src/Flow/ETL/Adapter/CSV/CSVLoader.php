<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
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
    /**
     * @var array<string, FileStream>
     */
    private array $resources;

    public function __construct(
        private readonly Path $path,
        private readonly bool $header = true,
        private readonly bool $safeMode = false,
        private string $separator = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
        private string $newLineSeparator = PHP_EOL
    ) {
        $this->resources = [];
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
        $this->resources = [];
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $headers = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());

        if (\count($context->partitionEntries())) {
            foreach ($rows->partitionBy(...$context->partitionEntries()) as $partitionedRows) {
                foreach ($partitionedRows->rows as $row) {
                    /**
                     * @psalm-suppress MixedArgumentTypeCoercion
                     * @phpstan-ignore-next-line
                     */
                    $this->writeRow($row->toArray(), $partitionedRows->partitions, $headers, $context);
                }
            }
        } else {
            foreach ($rows as $row) {
                /**
                 * @psalm-suppress MixedArgumentTypeCoercion
                 * @phpstan-ignore-next-line
                 */
                $this->writeRow($row->toArray(), [], $headers, $context);
            }
        }
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows, FlowContext $context) : void
    {
        foreach ($this->resources as $resource) {
            $resource->close();
        }
    }

    /**
     * @param array<array-key, null|scalar|\Stringable> $row
     * @param array<Partition> $partitions
     * @param array<string> $headers
     *
     * @throws RuntimeException
     */
    private function writeRow(array $row, array $partitions, array $headers, FlowContext $context) : void
    {
        $destination = \count($partitions)
            ? $this->path->addPartitions(...$partitions)
            : $this->path;

        if (!\array_key_exists($destination->uri(), $this->resources)) {
            $this->resources[$destination->uri()] = $context->fs()->open(
                ($this->safeMode || \count($partitions)) ? $destination->randomize()->setExtension('csv') : $destination,
                Mode::WRITE
            );

            if ($this->header) {
                /**
                 * @psalm-suppress TooManyArguments
                 * @psalm-suppress InvalidNamedArgument
                 */
                \fputcsv(
                    stream: $this->resources[$destination->uri()]->resource(),
                    fields: $headers,
                    separator: $this->separator,
                    enclosure: $this->enclosure,
                    escape: $this->escape,
                    eol: $this->newLineSeparator
                );
            }
        }

        /**
         * @psalm-suppress TooManyArguments
         * @psalm-suppress InvalidNamedArgument
         */
        \fputcsv(
            stream: $this->resources[$destination->uri()]->resource(),
            fields: $row,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator
        );
    }
}
