<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<bool, array{name: string, value: bool, type: ScalarType}>
 */
final class BooleanEntry implements \Stringable, Entry
{
    use EntryRef;

    private readonly ScalarType $type;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly bool $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->type = ScalarType::boolean();
    }

    public static function from(string $name, bool|int|string $value) : self
    {
        if (\is_bool($value)) {
            return new self($name, $value);
        }

        $value = \mb_strtolower(\trim((string) $value));

        if (!\in_array($value, ['1', '0', 'true', 'false', 'yes', 'no'], true)) {
            throw InvalidArgumentException::because('Value "%s" can\'t be casted to boolean.', $value);
        }

        if ($value === 'true' || $value === 'yes') {
            return new self($name, true);
        }

        if ($value === 'false' || $value === 'no') {
            return new self($name, false);
        }

        return new self($name, (bool) $value);
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value, 'type' => $this->type];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->type = $data['type'];
    }

    public function definition() : Definition
    {
        return Definition::boolean($this->name, $this->type->nullable());
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && $this->value() === $entry->value();
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        return $this->value() ? 'true' : 'false';
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : bool
    {
        return $this->value;
    }
}
