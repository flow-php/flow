<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration\Elasticsearch;

use Flow\ETL\Adapter\Seal\Tests\AbstractSealExtractorTestCase;
use Flow\ETL\Adapter\Seal\Tests\Backend\ElasticsearchBackend;

final class SealExtractorTest extends AbstractSealExtractorTestCase
{
    use ElasticsearchBackend;
}
