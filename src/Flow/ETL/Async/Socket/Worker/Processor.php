<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

use Flow\ETL\Flow;
use Flow\ETL\Loader;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Psr\Log\LoggerInterface;

final class Processor
{
    private Pipes $pipes;

    public function __construct(private readonly string $workerId, private readonly LoggerInterface $logger)
    {
        $this->pipes = Pipes::empty();
    }

    public function process(Rows $rows) : Rows
    {
        $memory = new Consumption();

        $this->logger->debug('processing', [
            'rows' => $rows->count(),
            'id' => $this->workerId,
        ]);

        $dataFrame = (new Flow())->process($rows);

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

    public function setPipes(Pipes $pipes) : void
    {
        $this->pipes = $pipes;
    }
}
