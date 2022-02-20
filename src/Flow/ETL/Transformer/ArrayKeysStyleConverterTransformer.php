<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\StyleConverter\ArrayKeyConverter;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Jawira\CaseConverter\Convert;

/**
 * @psalm-immutable
 */
final class ArrayKeysStyleConverterTransformer implements Transformer
{
    private string $arrayEntryName;

    private string $style;

    private EntryFactory $entryFactory;

    public function __construct(
        string $arrayEntryName,
        string $style,
        EntryFactory $entryFactory = null
    ) {
        /** @psalm-suppress ImpureFunctionCall */
        if (!\class_exists('Jawira\CaseConverter\Convert')) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please add jawira/case-converter dependency to the project first.");
        }

        if (!\in_array($style, StringStyles::ALL, true)) {
            throw new InvalidArgumentException("Unrecognized style {$style}, please use one of following: " . \implode(', ', StringStyles::ALL));
        }

        $this->arrayEntryName = $arrayEntryName;
        $this->style = $style;
        $this->entryFactory = $entryFactory ?? new NativeEntryFactory();
    }

    /**
     * @return array{array_entry_name: string, style: string, entry_factory: EntryFactory}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'style' => $this->style,
            'entry_factory' => $this->entryFactory,
        ];
    }

    /**
     * @param array{array_entry_name: string, style: string, entry_factory: EntryFactory} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->style = $data['style'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                $entryClass = \get_class($arrayEntry);

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            /**
             * @phpstan-ignore-next-line
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
