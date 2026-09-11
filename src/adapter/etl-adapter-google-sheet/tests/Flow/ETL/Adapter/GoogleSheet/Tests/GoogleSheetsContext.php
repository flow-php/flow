<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests;

use ArrayObject;
use Google\Client as GoogleClient;
use Google\Service\Sheets;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use RuntimeException;

use function file_get_contents;

final readonly class GoogleSheetsContext
{
    /**
     * Typed as Guzzle types the container it writes into; requests() narrows on the way out.
     *
     * @var ArrayObject<int, array<array-key, mixed>>
     */
    private ArrayObject $history;

    public function __construct(
        private GoogleClient $client = new GoogleClient(),
    ) {
        $this->history = new ArrayObject();
    }

    /**
     * @return list<RequestInterface> every request the client sent, in order
     */
    public function requests(): array
    {
        /** @var list<RequestInterface> */
        return array_column($this->history->getArrayCopy(), 'request');
    }

    /**
     * One fixture per PASS, each queued behind a `spreadsheet.json`, because every pass reads the grid size before
     * its values call. A cold undeclared read is two passes - the sample, then the batch - so it takes two
     * fixtures; a declared read is one pass and takes one. The queue is strictly FIFO.
     */
    public function sheets(string ...$fixtureFiles): Sheets
    {
        return $this->sheetsOfGrid(__DIR__ . '/Fixtures/spreadsheet.json', ...$fixtureFiles);
    }

    /**
     * The same queue over a chosen grid. A grid larger than the sample budget is what forces the read to be a real
     * second fetch rather than being served from the sample.
     */
    public function sheetsOfGrid(string $gridFixture, string ...$fixtureFiles): Sheets
    {
        $responses = [];

        foreach ($fixtureFiles as $fixtureFile) {
            foreach ([$gridFixture, $fixtureFile] as $file) {
                $responses[] = new Response(
                    200,
                    ['Content-Type' => 'application/json'],
                    file_get_contents($file) ?: throw new RuntimeException('Failed to read file: ' . $file),
                );
            }
        }

        // a local, not the property: history() takes its container by reference and Mago widens what it writes back
        $container = $this->history;
        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($container));

        $this->client->setHttpClient(new HttpClient(['handler' => $stack]));

        return new Sheets($this->client);
    }
}
