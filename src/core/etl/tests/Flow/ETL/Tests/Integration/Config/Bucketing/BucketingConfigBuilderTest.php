<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Config\Bucketing;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class BucketingConfigBuilderTest extends FlowIntegrationTestCase
{
    public function test_build_uses_the_given_spill_root_for_the_default_storage(): void
    {
        $spillRoot = $this->cacheDir->suffix('/spill-root');

        (new BucketingConfigBuilder('/flow-php-sort/', 64))
            ->build($spillRoot, new PhpBackend())
            ->storage->set('bucket-1', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertNotSame(
            [],
            iterator_to_array(
                $this->fs->list(path($spillRoot->path() . '/flow-php-sort/flow-php-buckets/**/*.floe')),
                false,
            ),
        );
    }
}
