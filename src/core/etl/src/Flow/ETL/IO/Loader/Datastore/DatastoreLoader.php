<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\Datastore;

use Flow\ETL\IO\Loader;

interface DatastoreLoader extends Loader
{
    public function collection(): string;
}
