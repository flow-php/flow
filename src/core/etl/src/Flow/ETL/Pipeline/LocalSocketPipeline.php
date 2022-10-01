<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Async\Socket\Server\Server;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Async\Socket\Worker\WorkerLauncher;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class LocalSocketPipeline implements Pipeline
{
    private Extractor $extractor;

    private readonly Pipes $pipes;

    private readonly int $totalWorkers;

    public function __construct(
        private readonly Server $server,
        private readonly WorkerLauncher $launcher,
        int $workers
    ) {
        if ($workers < 1) {
            throw new InvalidArgumentException("Number of workers can't be lower than 1, given: {$workers}");
        }

        $this->totalWorkers = $workers;
        $this->pipes = Pipes::empty();
        $this->extractor = new ProcessExtractor(new Rows());
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipes->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new Pipeline\SynchronousPipeline();
    }

    public function process(FlowContext $context) : \Generator
    {
        $threadSafeContext = $context->setThreadSafe();

        $plan = $threadSafeContext
            ->config
            ->processors()
            ->process(new Pipeline\Execution\LogicalPlan($this->extractor, $this->pipes), $threadSafeContext);

        $pool = Pool::generate($this->totalWorkers);

        $id = \uniqid('flow_async_pipeline', true);

        $this->server->initialize(new ServerProtocol($threadSafeContext, $id, $pool, $plan->extractor, $plan->pipes));

        $this->launcher->launch($pool, $this->server->host());

        $this->server->start();

        return $threadSafeContext->config->cache()->read($id);
    }

    public function source(Extractor $extractor) : self
    {
        $this->extractor = $extractor;

        return $this;
    }
}
