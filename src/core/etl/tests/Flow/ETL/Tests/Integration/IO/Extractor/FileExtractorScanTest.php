<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Extractor;

use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;

/**
 * The listing sources, which read the id from the path partition. Every file format runs the same case through
 * FileFormatContract in its own package.
 */
final class FileExtractorScanTest extends FlowIntegrationTestCase
{
    #[TestWith(['files'])]
    #[TestWith(['path_partitions'])]
    public function test_a_pushed_filter_returns_what_the_filter_node_returns(string $family): void
    {
        $directory = $this->cacheDir->path() . '/pushed-' . $family;
        $extractor = static fn(): FileExtractor => $family === 'files'
            ? files($directory . '/*/*')
            : from_path_partitions($directory . '/*/*');
        $predicate = ref('id')->notEquals(lit('b'));

        foreach (['a', 'b', 'c'] as $id) {
            df()
                ->read(from_array([['id' => $id]]))
                ->write(to_floe(path($directory . '/id=' . $id . '/rows.floe')))
                ->run();
        }

        $pushed = df()->read($extractor())->filter($predicate)->fetch();
        $plain = df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
            ->read($extractor())
            ->filter($predicate)
            ->fetch();

        static::assertCount(2, $plain);
        static::assertEquals($plain->toArray(), $pushed->toArray());
    }
}
