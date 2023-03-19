<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{case: string, refs: array<EntryReference>}>
 */
final class StringEntryValueCaseConverterTransformer implements Transformer
{
    private const CASE_LOWER = 'lower';

    private const CASE_UPPER = 'upper';

    /**
     * @var array<EntryReference>
     */
    private readonly array $refs;

    private function __construct(
        private readonly string $case,
        string|Reference ...$refs
    ) {
        $this->refs = EntryReference::initAll(...$refs);
    }

    public static function lower(string|Reference ...$entryNames) : self
    {
        return new self(self::CASE_LOWER, ...$entryNames);
    }

    public static function upper(string|Reference ...$entryNames) : self
    {
        return new self(self::CASE_UPPER, ...$entryNames);
    }

    public function __serialize() : array
    {
        return [
            'case' => $this->case,
            'refs' => $this->refs,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->case = $data['case'];
        $this->refs = $data['refs'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            foreach ($this->refs as $ref) {
                $entry = $row->get($ref);

                if ($entry instanceof Row\Entry\StringEntry) {
                    $row = $row->set(
                        new Row\Entry\StringEntry($entry->name(), ($this->case === self::CASE_UPPER) ? \mb_strtoupper($entry->value()) : \mb_strtolower($entry->value()))
                    );
                }
            }

            return $row;
        };

        return $rows->map($transformer);
    }
}
