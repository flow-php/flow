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
 * @implements Transformer<array{left_entry: string, right_value: int|float, operation: Operation|string, new_entry_name: string}>
 * @psalm-immutable
 */
final class MathValueOperationTransformer implements Transformer
{
    private function __construct(
        private readonly string $leftEntry,
        private readonly int|float $rightValue,
        private readonly Operation|string $operation,
        private readonly string $newEntryName
    ) {
    }

    public static function add(string $leftEntry, int|float $rightValue, string $newEntryName = 'add') : self
    {
        return new self($leftEntry, $rightValue, Operation::add, $newEntryName);
    }

    public static function divide(string $leftEntry, int|float $rightValue, string $newEntryName = 'divide') : self
    {
        return new self($leftEntry, $rightValue, Operation::divide, $newEntryName);
    }

    public static function modulo(string $leftEntry, int|float $rightValue, string $newEntryName = 'modulo') : self
    {
        return new self($leftEntry, $rightValue, Operation::modulo, $newEntryName);
    }

    public static function multiply(string $leftEntry, int|float $rightValue, string $newEntryName = 'multiply') : self
    {
        return new self($leftEntry, $rightValue, Operation::multiply, $newEntryName);
    }

    public static function power(string $leftEntry, int|float $rightValue, string $newEntryName = 'power') : self
    {
        return new self($leftEntry, $rightValue, Operation::power, $newEntryName);
    }

    public static function subtract(string $leftEntry, int|float $rightValue, string $newEntryName = 'subtract') : self
    {
        return new self($leftEntry, $rightValue, Operation::subtract, $newEntryName);
    }

    public function __serialize() : array
    {
        return [
            'left_entry' => $this->leftEntry,
            'right_value' => $this->rightValue,
            'operation' => $this->operation,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->leftEntry = $data['left_entry'];
        $this->rightValue = $data['right_value'];
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

            $left = $row->get($this->leftEntry);

            if (!$left instanceof Row\Entry\IntegerEntry && !$left instanceof Row\Entry\FloatEntry) {
                throw new RuntimeException("\"{$this->leftEntry}\" is not IntegerEntry or FloatEntry");
            }

            $operation = \is_string($this->operation)
                ? Operation::from($this->operation)
                : $this->operation;

            $value = match ($operation) {
                Operation::add => $left->value() + $this->rightValue,
                Operation::subtract => $left->value() - $this->rightValue,
                Operation::multiply => $left->value() * $this->rightValue,
                Operation::divide => $left->value() / $this->rightValue,
                Operation::modulo => $left->value() % $this->rightValue,
                Operation::power => $left->value() ** $this->rightValue,
                default => throw new RuntimeException('Unknown operation'),
            };

            if (\is_float($value)) {
                return $row->set(new Row\Entry\FloatEntry($this->newEntryName, $value));
            }

            return $row->set(new Row\Entry\IntegerEntry($this->newEntryName, $value));
        };

        return $rows->map($transformer);
    }
}
