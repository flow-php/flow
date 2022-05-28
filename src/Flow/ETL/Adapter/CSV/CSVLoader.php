<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\Mode;

/**
 * @implements Loader<array{
 *     stream: FileStream,
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
    private Handler $handler;

    /**
     * @var null|resource
     */
    private $resource;

    public function __construct(
        private readonly FileStream $stream,
        private readonly bool $header = true,
        private readonly bool $safeMode = false,
        private string $separator = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
        private string $newLineSeparator = PHP_EOL
    ) {
        $this->handler = $this->safeMode ? Handler::directory('csv') : Handler::file();
    }

    public function __serialize() : array
    {
        return [
            'stream' => $this->stream,
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
        $this->stream = $data['stream'];
        $this->header = $data['header'];
        $this->safeMode = $data['safe_mode'];
        $this->separator = $data['separator'];
        $this->escape = $data['escape'];
        $this->enclosure = $data['enclosure'];
        $this->newLineSeparator = $data['new_line_separator'];
        $this->handler = $this->safeMode ? Handler::directory('csv') : Handler::file();
        $this->resource = null;
    }

    public function load(Rows $rows) : void
    {
        if ($this->resource === null && $this->header) {
            $this->writeRow($rows->first()->entries()->map(fn (Entry $entry) => $entry->name()));
        }

        foreach ($rows as $row) {
            /**
             * @psalm-suppress MixedArgumentTypeCoercion
             * @phpstan-ignore-next-line
             */
            $this->writeRow($row->toArray());
        }
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows) : void
    {
        if (\is_resource($this->resource)) {
            \fclose($this->resource);
        }
    }

    /**
     * @throws RuntimeException
     *
     * @return resource
     * @psalm-suppress InvalidNullableReturnType
     */
    private function stream()
    {
        if ($this->resource === null) {
            $this->resource = $this->handler->open($this->stream, Mode::WRITE);
        }

        return $this->resource;
    }

    /**
     * @param array<array-key, null|scalar|\Stringable> $row
     *
     * @throws RuntimeException
     */
    private function writeRow(array $row) : void
    {
        /**
         * @psalm-suppress TooManyArguments
         * @psalm-suppress InvalidNamedArgument
         */
        \fputcsv(
            stream: $this->stream(),
            fields: $row,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator
        );
    }
}
