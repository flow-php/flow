<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

use Flow\ETL\CLI\CLI;
use Flow\ETL\CLI\Input;
use Psr\Log\LoggerInterface;

final class SocketLocalWorkerCLI implements CLI
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Client $client
    ) {
    }

    public function run(Input $input) : int
    {
        $host = $input->optionValue('host');
        $id = $input->optionValue('id');

        $this->logger->debug('[worker] arguments', ['id' => $id, 'host' => $host]);

        if ($host === null) {
            $this->logger->error('[worker] missing --host option', [
                'argv' => $input->argv(),
            ]);

            return 1;
        }

        if ($id === null) {
            $this->logger->error('[worker] missing --id option', [
                'argv' => $input->argv(),
            ]);

            return 1;
        }

        try {
            $this->client->connect($id, $host, new ClientProtocol(new Processor($id, $this->logger)));
        } catch (\Throwable $e) {
            $this->logger->error('[worker] something went wrong', [
                'exception' => $e,
                'tract' => $e->getTraceAsString(),
            ]);

            return 1;
        }

        return 0;
    }
}
