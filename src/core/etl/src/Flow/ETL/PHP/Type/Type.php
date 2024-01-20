<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

interface Type
{
    public function isEqual(self $type) : bool;

    public function isValid(mixed $value) : bool;

    public function makeNullable(bool $nullable) : self;

    public function merge(self $type) : self;

    public function nullable() : bool;

    public function toString() : string;
}
