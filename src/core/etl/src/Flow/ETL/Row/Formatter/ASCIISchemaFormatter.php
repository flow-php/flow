<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Formatter;

use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SchemaFormatter;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function array_merge;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function implode;
use function ksort;
use function str_repeat;

final readonly class ASCIISchemaFormatter implements SchemaFormatter
{
    public function __construct(
        private bool $asTable = false,
        private bool $withMetadata = true,
    ) {}

    public function format(Schema $schema): string
    {
        if ($this->asTable) {
            return (new AsciiTableFormatter())->format($this->table($schema), false);
        }

        /** @var array<string, string> $buffer */
        $buffer = [];

        foreach ($schema->definitions() as $definition) {
            $buffer = $this->formatEntry($definition, $buffer);
        }

        ksort($buffer);

        $output = "schema\n";
        $output .= implode("\n", $buffer);

        return $output . "\n";
    }

    private function table(Schema $schema): Rows
    {
        $rows = [];

        foreach ($schema->definitions() as $definition) {
            $values = [
                'name' => $definition->entry()->name(),
                'type' => $definition->type()->normalize()['type'],
                'nullable' => $definition->isNullable(),
            ];

            if ($this->withMetadata) {
                $values['metadata'] = $definition->metadata()->normalize();
            }

            $rows[] = new Row($values);
        }

        $columns = [str_schema('name'), str_schema('type'), bool_schema('nullable')];

        if ($this->withMetadata) {
            $columns[] = json_schema('metadata');
        }

        return new Rows(schema(...$columns), ...$rows);
    }

    /**
     * @param Definition<mixed> $definition
     * @param array<string> $buffer
     *
     * @return array<string>
     */
    private function formatEntry(Definition $definition, array $buffer): array
    {
        $entry = $definition->entry()->name();

        $indention = '';

        if ($definition->type() instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $entry . ': structure';

            /** @var StructureType<array<array-key, mixed>> $structureType */
            $structureType = $definition->type();

            $fields = [];

            foreach ($structureType->elements() as $element) {
                $fields += $this->formatStructureElement(
                    $element->optional ? $element->name . '?' : $element->name,
                    $element->type,
                    $fields,
                    1,
                );
            }

            $buffer = array_merge($buffer, $fields);
        } else {
            $buffer[] =
                $indention
                . '|-- '
                . $entry
                . ': '
                . ($definition->isNullable() ? '?' : '')
                . $definition->type()->toString();
        }

        return $buffer;
    }

    /**
     * @param Type<mixed> $structureType
     * @param array<int, string> $buffer
     * @param int<0, max> $level
     *
     * @return array<int, string>
     */
    private function formatStructureElement(int|string $name, Type $structureType, array $buffer, int $level): array
    {
        $indention = str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        if ($structureType instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $name . ': structure';

            $fields = [];

            foreach ($structureType->elements() as $element) {
                $fields += $this->formatStructureElement(
                    $element->optional ? $element->name . '?' : $element->name,
                    $element->type,
                    $fields,
                    $level + 1,
                );
            }

            $buffer = array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $name . ': ' . $structureType->toString();
        }

        return $buffer;
    }
}
