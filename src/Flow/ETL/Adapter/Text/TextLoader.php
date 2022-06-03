<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\Mode;

/**
 * @implements Loader<array{
 *     stream: FileStream,
 *     safe_mode: boolean,
 *     new_line_separator: string
 *  }>
 */
final class TextLoader implements Closure, Loader
{
    private Handler $handler;

    /**
     * @var null|resource
     */
    private $resource;

    public function __construct(
        private readonly FileStream $stream,
        private readonly bool $safeMode = false,
        private string $newLineSeparator = PHP_EOL,
    ) {
        $this->handler = $this->safeMode ? Handler::directory('txt') : Handler::file();
    }

    public function __serialize() : array
    {
        return [
            'stream' => $this->stream,
            'safe_mode' => $this->safeMode,
            'new_line_separator' => $this->newLineSeparator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->stream = $data['stream'];
        $this->safeMode = $data['safe_mode'];
        $this->newLineSeparator = $data['new_line_separator'];
        $this->handler = $this->safeMode ? Handler::directory('txt') : Handler::file();
        $this->resource = null;
    }

    public function load(Rows $rows) : void
    {
        foreach ($rows as $row) {
            if ($row->entries()->count() > 1) {
                throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
            }

            \fwrite($this->stream(), $row->entries()->all()[0]->toString() . $this->newLineSeparator);
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
}
