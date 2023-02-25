<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Math\Operation;

/**
 * @implements Transformer<array{left_entry: string, right_entry: string, operation: Operation|string, new_entry_name: string}>
 */
final class MathOperationTransformer implements Transformer
{
    private function __construct(
        private readonly string $leftEntry,
        private readonly string $rightEntry,
        private readonly Operation|string $operation,
        private readonly string $newEntryName
    ) {
    }

    public static function add(string $leftEntry, string $rightEntry, string $newEntryName = 'add') : self
    {
        return new self($leftEntry, $rightEntry, Operation::add, $newEntryName);
    }

    public static function divide(string $leftEntry, string $rightEntry, string $newEntryName = 'divide') : self
    {
        return new self($leftEntry, $rightEntry, Operation::divide, $newEntryName);
    }

    public static function modulo(string $leftEntry, string $rightEntry, string $newEntryName = 'modulo') : self
    {
        return new self($leftEntry, $rightEntry, Operation::modulo, $newEntryName);
    }

    public static function multiply(string $leftEntry, string $rightEntry, string $newEntryName = 'multiply') : self
    {
        return new self($leftEntry, $rightEntry, Operation::multiply, $newEntryName);
    }

    public static function power(string $leftEntry, string $rightEntry, string $newEntryName = 'power') : self
    {
        return new self($leftEntry, $rightEntry, Operation::power, $newEntryName);
    }

    public static function subtract(string $leftEntry, string $rightEntry, string $newEntryName = 'subtract') : self
    {
        return new self($leftEntry, $rightEntry, Operation::subtract, $newEntryName);
    }

    public function __serialize() : array
    {
        return [
            'left_entry' => $this->leftEntry,
            'right_entry' => $this->rightEntry,
            'operation' => $this->operation,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->leftEntry = $data['left_entry'];
        $this->rightEntry = $data['right_entry'];
        $this->operation = $data['operation'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
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

            $operation = \is_string($this->operation)
                ? Operation::from($this->operation)
                : $this->operation;

            $value = match ($operation) {
                Operation::add => $left->value() + $right->value(),
                Operation::subtract => $left->value() - $right->value(),
                Operation::multiply => $left->value() * $right->value(),
                Operation::divide => $left->value() / $right->value(),
                Operation::modulo => $left->value() % $right->value(),
                Operation::power => $left->value() ** $right->value(),
            };

            if (\is_float($value)) {
                return $row->set(new Row\Entry\FloatEntry($this->newEntryName, $value));
            }

            return $row->set(new Row\Entry\IntegerEntry($this->newEntryName, $value));
        };

        return $rows->map($transformer);
    }
}
