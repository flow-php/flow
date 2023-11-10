<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

/**
 * @implements NativeType<array{nullable: bool}>
 */
final class CallableType implements NativeType
{
    public function __construct(private readonly bool $nullable)
    {

    }

    public function __serialize() : array
    {
        return ['nullable' => $this->nullable];
    }

    public function __unserialize(array $data) : void
    {
        $this->nullable = $data['nullable'];
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->nullable === $type->nullable;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_callable($value);
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'callable';
    }
}
