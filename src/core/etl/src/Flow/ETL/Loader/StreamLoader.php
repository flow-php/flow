<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Formatter;
use Flow\ETL\Loader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\SchemaFormatter;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{url: string, mode: Mode, truncate: int|bool, output: Output, formatter: Formatter, schema_formatter: SchemaFormatter}>
 */
final class StreamLoader implements Loader
{
    /**
     * @param string $url all protocols supported by PHP are allowed https://www.php.net/manual/en/wrappers.php
     * @param Mode $mode only writing modes explained in https://www.php.net/manual/en/function.fopen.php are supported
     * @param bool|int $truncate if false or 0, then columns in display are not truncated
     * @param Formatter $formatter - if not passed AsciiTableFormatter is used
     */
    public function __construct(
        private readonly string $url,
        private readonly Mode $mode = Mode::WRITE,
        private readonly int|bool $truncate = 20,
        private readonly Output $output = Output::rows,
        private readonly Formatter $formatter = new Formatter\AsciiTableFormatter(),
        private readonly SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()
    ) {
    }

    public static function output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://output', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter);
    }

    public static function stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://stderr', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter);
    }

    public static function stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://stdout', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter);
    }

    public function __serialize() : array
    {
        return [
            'url' => $this->url,
            'mode' => $this->mode,
            'truncate' => $this->truncate,
            'output' => $this->output,
            'formatter' => $this->formatter,
            'schema_formatter' => $this->schemaFormatter,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->url = $data['url'];
        $this->mode = $data['mode'];
        $this->truncate = $data['truncate'];
        $this->output = $data['output'];
        $this->formatter = $data['formatter'];
        $this->schemaFormatter = $data['schema_formatter'];
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        try {
            $stream = @\fopen($this->url, $this->mode->value);
        } catch (\Throwable $e) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode->value}. Reason: " . $e->getMessage(), (int) $e->getCode(), $e);
        }

        if ($stream === false) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode->value}");
        }

        \fwrite(
            $stream,
            match ($this->output) {
                Output::rows => $this->formatter->format($rows, $this->truncate),
                Output::schema => $this->schemaFormatter->format($rows->schema()),
                Output::rows_and_schema => $this->formatter->format($rows, $this->truncate) . "\n" . $this->schemaFormatter->format($rows->schema())
            }
        );

        \fclose($stream);
    }
}
