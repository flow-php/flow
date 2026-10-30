<?php

declare(strict_types=1);

namespace Flow\ETL\Values;

use Flow\ETL\Values\Schema\Validator\ValidationContext;

interface SchemaValidator
{
    public function validate(Schema $expected, Schema $given): ValidationContext;
}
