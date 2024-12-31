<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\SchemaFormatter;
use Flow\ETL\{FlowContext, Formatter, Loader, Loader\StreamLoader\Type, Rows};
use Flow\Filesystem\Stream\Mode;

final class StreamLoader implements Closure, Loader
{
    /**
     * @var null|resource
     */
    private $stream;

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
        private readonly SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter(),
        private readonly Type $type = Type::custom,
    ) {
        $this->stream = null;
    }

    public static function output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://output', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter, Type::output);
    }

    public static function stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://stderr', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter, Type::stderr);
    }

    public static function stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : self
    {
        return new self('php://stdout', Mode::WRITE, $truncate, $output, $formatter, $schemaFormatter, Type::stdout);
    }

    public function closure(FlowContext $context) : void
    {
        $this->closeStream();
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $stream = $this->getStream();

        \fwrite(
            $stream,
            match ($this->output) {
                Output::rows_count => 'Rows: ' . $rows->count() . "\n",
                Output::column_count => 'Columns: ' . $rows->schema()->count() . "\n",
                Output::rows_and_column_count => 'Rows: ' . $rows->count() . ', Columns: ' . $rows->schema()->count() . "\n",
                Output::rows => $this->formatter->format($rows, $this->truncate),
                Output::schema => $this->schemaFormatter->format($rows->schema()),
                Output::rows_and_schema => $this->formatter->format($rows, $this->truncate) . "\n" . $this->schemaFormatter->format($rows->schema()),
            }
        );

        match ($this->type) {
            Type::output => $this->closeStream(),
            Type::stderr => $this->closeStream(),
            Type::stdout => $this->closeStream(),
            Type::custom => null,
        };
    }

    private function closeStream() : void
    {
        if ($this->stream !== null) {
            \fclose($this->stream);
            $this->stream = null;
        }
    }

    /**
     * @return resource
     */
    private function getStream()
    {
        if ($this->stream !== null) {
            return $this->stream;
        }

        try {
            /** @phpstan-ignore-next-line */
            $this->stream = @\fopen($this->url, $this->mode->value);
        } catch (\Throwable $e) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode->value}. Reason: " . $e->getMessage(), (int) $e->getCode(), $e);
        }

        if ($this->stream === false) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode->value}");
        }

        return $this->stream;
    }
}
