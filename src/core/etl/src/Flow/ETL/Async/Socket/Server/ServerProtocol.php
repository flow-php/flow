<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Server;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Communication\Protocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;

final class ServerProtocol
{
    public static bool $locked = false;

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    private \Generator $generator;

    public function __construct(
        private readonly FlowContext $context,
        private readonly string $cacheId,
        private readonly Pool $workers,
        private readonly Extractor $extractor,
        private readonly Pipes $pipes
    ) {
        $this->generator = $this->extractor->extract($this->context);
    }

    public function handle(Message $message, Client $client, Server $server) : void
    {
        switch ($message->type()) {
            case Protocol::CLIENT_IDENTIFY:
                if ($this->workers->has($message->payload()['id'] ?? '')) {
                    $this->workers->connect($message->payload()['id'] ?? '');
                    $client->send(Message::setup($this->pipes, $this->context->config->cache(), $this->cacheId));
                } else {
                    $client->disconnect();
                }

                break;
            case Protocol::CLIENT_FETCH:
                $this->sendNextRows($message, $client);

                break;
        }

        if ($this->workers->onlyConnected()->count() === 0 && !$this->generator->valid()) {
            if ($server->isRunning()) {
                $server->stop();
            }
        }
    }

    private function sendNextRows(Message $message, Client $client) : void
    {
        if ($this->generator->valid()) {
            /** @var Rows $rows */
            $rows = $this->generator->current();
            $this->generator->next();

            $message = Message::process($rows);
            $client->send($message);
        } else {
            $client->disconnect();
            $this->workers->disconnect($message->payload()['id'] ?? '');
        }
    }
}
