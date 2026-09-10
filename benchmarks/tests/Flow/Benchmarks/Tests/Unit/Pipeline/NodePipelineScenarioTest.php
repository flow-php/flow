<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Datasets\Paths;
use Flow\Benchmarks\Pipeline\NodeFixture;
use Flow\Benchmarks\Pipeline\NodePipelineScenario;
use Flow\Benchmarks\Pipeline\NodeSource;
use Flow\Benchmarks\Tests\Context\WrittenFile;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function file_get_contents;
use function substr_count;

final class NodePipelineScenarioTest extends TestCase
{
    public const ROWS = 100;

    /**
     * The sink is line-oriented but an XML node stringifies to many lines, so the row count is
     * counted by a per-row marker rather than by lines.
     */
    public static function sources(): Generator
    {
        yield 'xml' => [NodeSource::xml, '<row>'];

        yield 'text' => [NodeSource::text, ' |'];
    }

    #[DataProvider('sources')]
    public function test_it_writes_one_record_per_source_row(NodeSource $source, string $marker): void
    {
        (new NodeFixture($source, self::ROWS))->warm();

        $written = new WrittenFile(Paths::var() . '/pipeline_' . $source->value . '_*.txt');
        (new NodePipelineScenario($source, self::ROWS))->run();

        $contents = (string) file_get_contents($written->path());
        $written->remove();

        static::assertSame(self::ROWS, substr_count($contents, $marker));
    }
}
