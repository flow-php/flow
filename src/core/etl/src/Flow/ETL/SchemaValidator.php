<?php

declare(strict_types=1);

namespace Flow\ETL;

interface SchemaValidator
{
    public function isValid(Schema $given, Schema $expected) : bool;
}
