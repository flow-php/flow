<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;

use function array_map;
use function count;
use function Flow\Types\DSL\type_bare;
use function implode;
use function is_resource;
use function preg_grep;
use function preg_match;
use function preg_quote;
use function str_repeat;

final class PhpCSVEncoder implements CSVEncoder
{
    /**
     * @var null|resource
     */
    private $buffer = null;

    private readonly string $enclosingCharacters;

    private readonly bool $scanNumbers;

    private readonly TextValues $text;

    public function __construct(
        private readonly CSVWriteOptions $options,
    ) {
        $special = $options->separator . $options->enclosure . $options->escape;
        $this->enclosingCharacters = '/[' . preg_quote($special, '/') . '\n\r\t ]/';
        $this->scanNumbers = preg_match('/[0-9A-Za-z_+.\/:\x80-\xFF-]/', $special) === 1;
        $this->text = new TextValues($options->dateTimeFormat, $options->dateFormat);
    }

    /**
     * @param Type<mixed> $type
     *
     * @return list<?string> the fields before quoting
     */
    public function cells(Type $type, Column $column): array
    {
        return $this->text->texts($type, $column->physicals());
    }

    public function encode(Rows $rows): string
    {
        $count = $rows->count();

        if ($count < 1) {
            return '';
        }

        $eol = $this->options->newLineSeparator;
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $cells = $this->cells($definition->type(), $rows->column($definition->entry()->name()));
            $bare = type_bare($definition->type());

            if (
                $this->scanNumbers
                || !(
                    $bare instanceof IntegerType
                    || $bare instanceof FloatType
                    || $bare instanceof BooleanType
                    || $bare instanceof TimeType
                    || $bare instanceof UuidType
                    || $bare instanceof EnumType
                    || $bare instanceof TimeZoneType
                )
            ) {
                // preg_grep() reads a null field as ''
                // @mago-ignore analysis:possibly-invalid-argument
                foreach (preg_grep($this->enclosingCharacters, $cells) ?: [] as $i => $cell) {
                    $cells[$i] = $this->line([$cell], '');
                }
            }

            $columns[] = $cells;
        }

        if ($columns === []) {
            return str_repeat($eol, $count);
        }

        if (count($columns) === 1) {
            return implode($eol, $columns[0]) . $eol;
        }

        $lines = '';

        foreach (array_map(null, ...$columns) as $row) {
            $lines .= implode($this->options->separator, $row) . $eol;
        }

        return $lines;
    }

    public function encodeHeader(array $headers): string
    {
        return $this->line($headers, $this->options->newLineSeparator);
    }

    /**
     * @param list<string> $fields
     */
    private function line(array $fields, string $eol): string
    {
        $buffer = $this->buffer();
        ftruncate($buffer, 0);
        rewind($buffer);

        fputcsv(
            stream: $buffer,
            fields: $fields,
            separator: $this->options->separator,
            enclosure: $this->options->enclosure,
            escape: $this->options->escape,
            eol: $eol,
        );

        $line = stream_get_contents($buffer, offset: 0);

        if ($line === false) {
            throw new RuntimeException('Failed to render a CSV line');
        }

        return $line;
    }

    /**
     * @return resource
     */
    private function buffer()
    {
        if (is_resource($this->buffer)) {
            return $this->buffer;
        }

        $buffer = fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

        if ($buffer === false) {
            throw new RuntimeException('Failed to open a temporary CSV buffer');
        }

        return $this->buffer = $buffer;
    }
}
