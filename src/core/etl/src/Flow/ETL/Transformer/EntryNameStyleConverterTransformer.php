<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Jawira\CaseConverter\Convert;

/**
 * @implements Transformer<array{style: string}>
 *
 * @psalm-immutable
 */
final class EntryNameStyleConverterTransformer implements Transformer
{
    public function __construct(private readonly string $style)
    {
        /** @psalm-suppress ImpureFunctionCall */
        if (!\class_exists(Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please add jawira/case-converter dependency to the project first.");
        }

        if (!\in_array($style, StringStyles::ALL, true)) {
            throw new InvalidArgumentException("Unrecognized style {$style}, please use one of following: " . \implode(', ', StringStyles::ALL));
        }
    }

    public function __serialize() : array
    {
        return [
            'style' => $this->style,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->style = $data['style'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @psalm-var pure-callable(Row $row) : Row $rowTransformer */
        $rowTransformer = function (Row $row) : Row {
            /** @psalm-var pure-callable(Entry) : Entry $valueMap */
            $valueMap = fn (Entry $entry) : Entry => $entry->rename(
                /** @phpstan-ignore-next-line */
                (string) \call_user_func([new Convert($entry->name()), 'to' . \ucfirst($this->style)])
            );

            return $row->map($valueMap);
        };

        return $rows->map($rowTransformer);
    }
}
