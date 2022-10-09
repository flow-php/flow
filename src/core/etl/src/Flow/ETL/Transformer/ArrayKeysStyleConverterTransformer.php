<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\StyleConverter\ArrayKeyConverter;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Jawira\CaseConverter\Convert;

/**
 * @implements Transformer<array{array_entry_name: string, style: string, entry_factory: EntryFactory}>
 */
final class ArrayKeysStyleConverterTransformer implements Transformer
{
    public function __construct(
        private readonly string $arrayEntryName,
        private readonly string $style,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
        /** @psalm-suppress ImpureFunctionCall */
        if (!\class_exists(\Jawira\CaseConverter\Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please add jawira/case-converter dependency to the project first.");
        }

        if (!\in_array($style, StringStyles::ALL, true)) {
            throw new InvalidArgumentException("Unrecognized style {$style}, please use one of following: " . \implode(', ', StringStyles::ALL));
        }
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'style' => $this->style,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->style = $data['style'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                $entryClass = $arrayEntry::class;

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            /**
             * @phpstan-ignore-next-line
             *
             * @psalm-var pure-callable(string) : string $converter
             */
            $converter = fn (string $key) : string => (string) \call_user_func([new Convert($key), 'to' . \ucfirst($this->style)]);

            return $row->set(
                $this->entryFactory->create(
                    $arrayEntry->name(),
                    (new ArrayKeyConverter($converter))->convert($arrayEntry->value())
                )
            );
        };

        return $rows->map($transformer);
    }
}
