<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Schema\Validator\ValidationContext;

interface SchemaValidator
{
    public function validate(Schema $expected, Schema $given): ValidationContext;
}
