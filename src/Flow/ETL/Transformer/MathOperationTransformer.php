<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class MathOperationTransformer implements Transformer
{
    private string $leftEntry;

    private string $newEntryName;

    private string $operation;

    private string $rightEntry;

    private function __construct(string $leftEntry, string $rightEntry, string $operation, string $newEntryName)
    {
        $this->leftEntry = $leftEntry;
        $this->rightEntry = $rightEntry;
        $this->operation = $operation;
        $this->newEntryName = $newEntryName;
    }

    public static function add(string $leftEntry, string $rightEntry, string $newEntryName = 'add') : self
    {
        return new self($leftEntry, $rightEntry, 'add', $newEntryName);
    }

    public static function divide(string $leftEntry, string $rightEntry, string $newEntryName = 'divide') : self
    {
        return new self($leftEntry, $rightEntry, 'divide', $newEntryName);
    }

    public static function modulo(string $leftEntry, string $rightEntry, string $newEntryName = 'modulo') : self
    {
        return new self($leftEntry, $rightEntry, 'modulo', $newEntryName);
    }

    public static function multiply(string $leftEntry, string $rightEntry, string $newEntryName = 'multiply') : self
    {
        return new self($leftEntry, $rightEntry, 'multiply', $newEntryName);
    }

    public static function power(string $leftEntry, string $rightEntry, string $newEntryName = 'power') : self
    {
        return new self($leftEntry, $rightEntry, 'power', $newEntryName);
    }

    public static function subtract(string $leftEntry, string $rightEntry, string $newEntryName = 'subtract') : self
    {
        return new self($leftEntry, $rightEntry, 'subtract', $newEntryName);
    }

    /**
     * @return array{left_entry: string, right_entry: string, operation: string, new_entry_name: string}
     */
    public function __serialize() : array
    {
        return [
            'left_entry' => $this->leftEntry,
            'right_entry' => $this->rightEntry,
            'operation' => $this->operation,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    /**
     * @param array{left_entry: string, right_entry: string, operation: string, new_entry_name: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->leftEntry = $data['left_entry'];
        $this->rightEntry = $data['right_entry'];
        $this->operation = $data['operation'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->leftEntry)) {
                throw new RuntimeException("\"{$this->leftEntry}\" not found");
            }

            if (!$row->entries()->has($this->rightEntry)) {
                throw new RuntimeException("\"{$this->rightEntry}\" not found");
            }

            $left = $row->get($this->leftEntry);
            $right = $row->get($this->rightEntry);

            if (!$left instanceof Row\Entry\IntegerEntry && !$left instanceof Row\Entry\FloatEntry) {
                throw new RuntimeException("\"{$this->leftEntry}\" is not IntegerEntry or FloatEntry");
            }

            if (!$right instanceof Row\Entry\IntegerEntry && !$right instanceof Row\Entry\FloatEntry) {
                throw new RuntimeException("\"{$this->rightEntry}\" is not IntegerEntry or FloatEntry");
            }

            switch ($this->operation) {
                case 'add':
                    $value = $left->value() + $right->value();

                    break;
                case 'subtract':
                    $value = $left->value() - $right->value();

                    break;
                case 'multiply':
                    $value = $left->value() * $right->value();

                    break;
                case 'divide':
                    $value = $left->value() / $right->value();

                    break;
                case 'modulo':
                    $value = $left->value() % $right->value();

                    break;
                case 'power':
                    $value = $left->value() ** $right->value();

                    break;

                default:
                    throw new RuntimeException('Unknown operation');
            }

            return $row->add(
                (\is_float($value))
                    ? new Row\Entry\FloatEntry($this->newEntryName, $value)
                    : new Row\Entry\IntegerEntry($this->newEntryName, $value)
            );
        };

        return $rows->map($transformer);
    }
}
