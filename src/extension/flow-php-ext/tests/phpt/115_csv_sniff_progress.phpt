--TEST--
RustCSVOpenSource::sniff() reports the folded rows to a Progress in ColumnTypes::sniff() chunks, native and PHP fold alike
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Types\Type\Native\String\StringTypeNarrower;

final class AdvancedLog implements Progress
{
    public array $advanced = [];

    public function advanced(int $rows): void
    {
        $this->advanced[] = $rows;
    }

    public function busy(): void
    {
    }

    public function countsIn(Flow\ETL\Foundation\ProgressUnit $unit): void
    {
    }

    public function done(): void
    {
    }

    public function reported(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes, bool $progressed): void
    {
    }

    public function reportsEverySeconds(): ?float
    {
        return null;
    }

    public function ended(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes): void
    {
    }
}

$csv = "id\n" . implode("\n", range(1, 2500)) . "\n";
$native = new StringTypeNarrower(InferredTypes::default()->toArray());
$php = new StringTypeNarrower([...InferredTypes::default()->toArray(), Flow\Types\DSL\type_html()]);

foreach (['native' => $native, 'php' => $php] as $label => $typer) {
    foreach ([[null, null], [1200, null], [0, null], [null, 4096]] as [$rowBudget, $chunk]) {
        $progress = new AdvancedLog();
        (new RustCSVOpenSource(new MemorySourceStream($csv), ',', '"', '', true, false, false, $chunk))
            ->sniff(['id'], $rowBudget, $typer, $progress);
        echo $label, ' ', $rowBudget ?? 'null', $chunk === null ? '' : " in {$chunk}-byte chunks", ': [', implode(', ', $progress->advanced), "]\n";
    }
}
--EXPECT--
native null: [1000, 1000, 500]
native 1200: [1000, 200]
native 0: []
native null in 4096-byte chunks: [1000, 1000, 500]
php null: [1000, 1000, 500]
php 1200: [1000, 200]
php 0: []
php null in 4096-byte chunks: [1000, 1000, 500]
