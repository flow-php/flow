<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_values;
use function count;
use function Flow\Types\DSL\type_bare;
use function implode;
use function sprintf;
use function str_repeat;

final class TextEncoder
{
    public function __construct(
        private readonly string $newLineSeparator = PHP_EOL,
    ) {}

    public function encode(Rows $rows): string
    {
        $count = $rows->count();

        if ($count < 1) {
            return '';
        }

        $definitions = array_values($rows->schema()->definitions());

        if (count($definitions) > 1) {
            throw new RuntimeException(sprintf(
                'Text data loader writes at most one column, the batch has %d columns.',
                count($definitions),
            ));
        }

        if ($definitions === []) {
            return str_repeat($this->newLineSeparator, $count);
        }

        $type = $definitions[0]->type();
        $bare = type_bare($type);

        if ($bare instanceof ListType || $bare instanceof MapType || $bare instanceof StructureType) {
            throw new RuntimeException('Text data loader supports only scalar values, got array');
        }

        return (
            implode(
                $this->newLineSeparator,
                (new TextValues())->texts($type, $rows->column($definitions[0]->entry()->name())->physicals()),
            ) . $this->newLineSeparator
        );
    }
}
