<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class ResourceType implements NativeType
{
    public function __construct(private readonly bool $nullable)
    {

    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->nullable === $type->nullable;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_resource($value);
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'resource';
    }
}
