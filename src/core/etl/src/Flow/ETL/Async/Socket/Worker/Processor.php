<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

use Flow\ETL\Config;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Loader;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Psr\Log\LoggerInterface;

final class Processor
{
    private ?Config $config;

    /**
     * @var array<EntryReference>
     */
    private array $partitionEntries;

    private PartitionFilter $partitionFilter;

    private Pipes $pipes;

    public function __construct(private readonly string $workerId, private readonly LoggerInterface $logger)
    {
        $this->pipes = Pipes::empty();
        $this->partitionFilter = new NoopFilter();
        $this->partitionEntries = [];
        $this->config = null;
    }

    public function process(Rows $rows) : Rows
    {
        $memory = new Consumption();

        $this->logger->debug('processing', [
            'rows' => $rows->count(),
            'id' => $this->workerId,
        ]);

        $dataFrame = (new Flow($this->config()))
            ->process($rows)
            ->filterPartitions($this->partitionFilter)
            /**
             * At the worker level only Append mode is accepted as true mode is set at the server level.
             * So for example if server is set to Overwrite and worker would also get Overwrite
             * each worker would remove all files created by other workers.
             */
            ->mode(SaveMode::Append)
            ->threadSafe();

        if ([] !== $this->partitionEntries) {
            $dataFrame = $dataFrame->partitionBy(...$this->partitionEntries);
        }

        foreach ($this->pipes->all() as $pipe) {
            if ($pipe instanceof Transformer) {
                $dataFrame = $dataFrame->transform($pipe);
            } elseif ($pipe instanceof Loader) {
                $dataFrame = $dataFrame->load($pipe);
            }
        }

        $result = $dataFrame->fetch();

        $this->logger->debug('processed', [
            'id' => $this->workerId,
            'rows' => $rows->count(),
            'transformer_rows' => $result->count(),
            'memory_mb' => $memory->current()->inMb(),
        ]);

        return $result;
    }

    /**
     * @param array<EntryReference> $partitionEntries
     */
    public function setPartitionEntries(array $partitionEntries) : void
    {
        $this->partitionEntries = $partitionEntries;
    }

    public function setPartitionFilter(PartitionFilter $partitionFilter) : void
    {
        $this->partitionFilter = $partitionFilter;
    }

    public function setPipes(Pipes $pipes) : void
    {
        $this->pipes = $pipes;
    }

    private function config() : Config
    {
        if ($this->config !== null) {
            return $this->config;
        }

        $this->config = Config::default();

        return $this->config;
    }
}
