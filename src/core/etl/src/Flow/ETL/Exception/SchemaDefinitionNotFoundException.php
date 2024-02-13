<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class SchemaDefinitionNotFoundException extends InvalidArgumentException
{
    public function __construct(private string $entry)
    {
        parent::__construct(\sprintf('Schema definition for entry "%s" not found', $entry));
    }

    public function entry() : string
    {
        return $this->entry;
    }
}
