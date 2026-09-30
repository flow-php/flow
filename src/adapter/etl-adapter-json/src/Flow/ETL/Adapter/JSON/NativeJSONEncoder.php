<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class NativeJSONEncoder implements JSONEncoder
{
    private ?Schema $schema = null;

    /**
     * @var list<string>
     */
    private array $unrendered = [];

    public function __construct(
        private readonly NativeJsonWriter $writer,
        private readonly PhpJSONEncoder $php,
    ) {}

    public function encode(Rows $rows, string $separator): string
    {
        $schema = $rows->schema();

        // consecutive batches share one Schema object
        if ($schema !== $this->schema) {
            $this->unrendered = $this->writer->unrendered($schema);
            $this->schema = $schema;
        }

        $fragments = [];

        foreach ($this->unrendered as $name) {
            $fragments[$name] = $this->php->fragments($schema->get($name)->type(), $rows->column($name));
        }

        return $this->writer->encode($rows, $fragments, $separator);
    }
}
