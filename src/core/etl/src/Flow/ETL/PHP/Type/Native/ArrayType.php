<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

/**
 * @implements NativeType<array{empty: bool, nullable: bool}>
 */
final class ArrayType implements NativeType
{
    public function __construct(private readonly bool $empty = false, private readonly bool $nullable = false)
    {
    }

    public static function empty() : self
    {
        return new self(true);
    }

    public function __serialize() : array
    {
        return [
            'empty' => $this->empty,
            'nullable' => $this->nullable,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->empty = $data['empty'];
        $this->nullable = $data['nullable'];
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->empty === $type->empty;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_array($value);
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        if ($this->empty) {
            return ($this->nullable ? '?' : '') . 'array<empty, empty>';
        }

        return ($this->nullable ? '?' : '') . 'array<mixed>';
    }
}
