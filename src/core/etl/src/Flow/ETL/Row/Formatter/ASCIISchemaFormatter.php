<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Formatter;

use function Flow\ETL\DSL\{df, from_array, ref, rename_replace, to_output};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\{Definition, SchemaFormatter};
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

final readonly class ASCIISchemaFormatter implements SchemaFormatter
{
    public function __construct(private bool $asTable = false, private bool $withMetadata = true)
    {
    }

    public function format(Schema $schema) : string
    {
        if ($this->asTable) {
            ob_start();
            $df = df()
                ->read(from_array($schema->normalize()))
                ->withEntry('type', ref('type')->unpack())
                ->renameEach(rename_replace('type.', ''))
                ->rename('ref', 'name')
                ->collect()
                ->select('name', 'type', 'nullable', 'metadata');

            if (!$this->withMetadata) {
                $df->drop('metadata');
            }

            $df->write(to_output(false))
                ->run();

            $content = ob_get_clean();

            if ($content === false) {
                throw new RuntimeException('Failed to get output buffer content');
            }

            return $content;
        }

        /** @var array<string, string> $buffer */
        $buffer = [];

        foreach ($schema->definitions() as $definition) {
            $buffer = $this->formatEntry($definition, $buffer);
        }

        \ksort($buffer);

        $output = "schema\n";
        $output .= \implode("\n", $buffer);

        return $output . "\n";
    }

    /**
     * @param Definition<mixed> $definition
     * @param array<string> $buffer
     *
     * @return array<string>
     */
    private function formatEntry(Definition $definition, array $buffer) : array
    {
        $entry = $definition->entry()->name();

        $indention = '';

        if ($definition->type() instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $entry . ': structure';

            /** @var StructureType<array<string, Type<mixed>>> $structureType */
            $structureType = $definition->type();

            $fields = [];

            foreach ($structureType->elements() as $name => $type) {
                $fields += $this->formatStructureElement($name, $type, $fields, 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $entry . ': ' . ($definition->isNullable() ? '?' : '') . $definition->type()->toString();
        }

        return $buffer;
    }

    /**
     * @param Type<mixed> $structureType
     * @param array<int, string> $buffer
     *
     * @return array<int, string>
     */
    private function formatStructureElement(string $name, Type $structureType, array $buffer, int $level) : array
    {

        $indention = \str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        if ($structureType instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $name . ': structure';

            $fields = [];

            foreach ($structureType->elements() as $nextName => $nextType) {
                $fields += $this->formatStructureElement($nextName, $nextType, $fields, $level + 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $name . ': ' . $structureType->toString();
        }

        return $buffer;
    }
}
