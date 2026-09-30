<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class NativeCSVEncoder implements CSVEncoder
{
    private ?Schema $schema = null;

    /**
     * @var list<string>
     */
    private array $unrendered = [];

    public function __construct(
        private readonly NativeCSVWriter $writer,
        private readonly PhpCSVEncoder $php,
    ) {}

    public function encode(Rows $rows): string
    {
        $schema = $rows->schema();

        // consecutive batches share one Schema object
        if ($schema !== $this->schema) {
            $this->unrendered = $this->writer->unrendered($schema);
            $this->schema = $schema;
        }

        $cells = [];

        foreach ($this->unrendered as $name) {
            $cells[$name] = $this->php->cells($schema->get($name)->type(), $rows->column($name));
        }

        return $this->writer->encode($rows, $cells);
    }

    public function encodeHeader(array $headers): string
    {
        return $this->php->encodeHeader($headers);
    }
}
