<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

interface Type
{
    public static function fromString(string $value) : self;

    public function isEqual(self $type) : bool;

    public function isValid(mixed $value) : bool;

    public function toString() : string;
}
