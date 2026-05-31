<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration\Elasticsearch;

use Flow\ETL\Adapter\Seal\Tests\AbstractSealLoaderTestCase;
use Flow\ETL\Adapter\Seal\Tests\Backend\ElasticsearchBackend;

final class SealLoaderTest extends AbstractSealLoaderTestCase
{
    use ElasticsearchBackend;
}
