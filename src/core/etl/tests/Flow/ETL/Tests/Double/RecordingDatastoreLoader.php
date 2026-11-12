<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader\Datastore\DatastoreLoader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final class RecordingDatastoreLoader implements DatastoreLoader
{
    public int $collectionCalls = 0;

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function collection(): string
    {
        $this->collectionCalls++;

        return 'crm.contacts';
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function load(Rows $rows, Backend $backend): void {}
}
