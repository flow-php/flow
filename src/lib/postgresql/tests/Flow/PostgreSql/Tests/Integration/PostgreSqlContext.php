<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration;

use function Flow\PostgreSql\DSL\{drop, pgsql_client, pgsql_connection_dsn};

use Flow\PostgreSql\Client\Client;

final class PostgreSqlContext
{
    public readonly Client $client;

    /** @var list<resource> */
    private array $backgroundProcesses = [];

    /** @var list<string> */
    private array $backgroundStderrLogs = [];

    private readonly string $dsn;

    /** @var list<Client> */
    private array $secondaryClients = [];

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->dsn = $dsn;
        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    /**
     * @return list<string>
     */
    public function backgroundStderrContents() : array
    {
        $out = [];

        foreach ($this->backgroundStderrLogs as $path) {
            if (\is_file($path)) {
                $out[] = \file_get_contents($path) ?: '';
            }
        }

        return $out;
    }

    public function client() : Client
    {
        return $this->client;
    }

    public function close() : void
    {
        foreach ($this->backgroundProcesses as $process) {
            \proc_close($process);
        }
        $this->backgroundProcesses = [];

        foreach ($this->backgroundStderrLogs as $path) {
            if (\is_file($path)) {
                @\unlink($path);
            }
        }
        $this->backgroundStderrLogs = [];

        foreach ($this->secondaryClients as $client) {
            $client->close();
        }
        $this->secondaryClients = [];

        $this->client->close();
    }

    public function dropDomainIfExists(string $domain) : void
    {
        $this->client->execute(drop()->domain($domain)->ifExists()->cascade()->toSql());
    }

    public function dropFunctionIfExists(string $function) : void
    {
        $this->client->execute(drop()->function($function)->ifExists()->cascade()->toSql());
    }

    public function dropIndexIfExists(string $index) : void
    {
        $this->client->execute(drop()->index($index)->ifExists()->cascade()->toSql());
    }

    public function dropMaterializedViewIfExists(string $view) : void
    {
        $this->client->execute(drop()->materializedView($view)->ifExists()->cascade()->toSql());
    }

    public function dropSchemaIfExists(string $schema) : void
    {
        $this->client->execute(drop()->schema($schema)->ifExists()->cascade()->toSql());
    }

    public function dropSequenceIfExists(string $sequence) : void
    {
        $this->client->execute(drop()->sequence($sequence)->ifExists()->cascade()->toSql());
    }

    public function dropTableIfExists(string $table) : void
    {
        $this->client->execute(drop()->table($table)->ifExists()->cascade()->toSql());
    }

    public function dropTriggerIfExists(string $trigger, string $table) : void
    {
        $this->client->execute(drop()->trigger($trigger)->on($table)->ifExists()->cascade()->toSql());
    }

    public function dropTypeIfExists(string $type) : void
    {
        $this->client->execute(drop()->type($type)->ifExists()->cascade()->toSql());
    }

    public function dropViewIfExists(string $view) : void
    {
        $this->client->execute(drop()->view($view)->ifExists()->cascade()->toSql());
    }

    /**
     * Opens an additional independent Client connected to the same database.
     * Used by tests that need two sessions (e.g. LISTEN/NOTIFY between
     * separate connections). The context owns the returned client and closes
     * it during tearDown.
     */
    public function newClient() : Client
    {
        $client = pgsql_client(pgsql_connection_dsn($this->dsn));
        $this->secondaryClients[] = $client;

        return $client;
    }

    /**
     * Spawns a detached child PHP process that sleeps for $delayMs and then
     * fires a NOTIFY on $channel with $payload using its own Client
     * connection. Returns the child process handle; the context closes it
     * during close(). Used for integration tests that need to exercise the
     * blocking wait path of waitForNotification().
     */
    public function spawnBackgroundNotifier(string $channel, string $payload, int $delayMs) : void
    {
        $autoload = \getcwd() . '/vendor/autoload.php';

        if (!\is_file($autoload)) {
            throw new \RuntimeException(\sprintf('Project vendor/autoload.php not found at %s', $autoload));
        }

        $phpCode = \sprintf(
            'usleep(%d); require %s; try { $c = \Flow\PostgreSql\DSL\pgsql_client(\Flow\PostgreSql\DSL\pgsql_connection_dsn(%s)); $c->execute(\Flow\PostgreSql\DSL\notify(%s)->withPayload(%s)); $c->close(); } catch (\Throwable $e) { fwrite(STDERR, "child error: " . get_class($e) . ": " . $e->getMessage() . "\n"); exit(1); }',
            $delayMs * 1000,
            \var_export($autoload, true),
            \var_export($this->dsn, true),
            \var_export($channel, true),
            \var_export($payload, true),
        );

        $stderrLog = \sys_get_temp_dir() . '/flow-bg-notifier-' . \uniqid('', true) . '.log';
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['file', '/dev/null', 'w'],
            2 => ['file', $stderrLog, 'w'],
        ];

        $process = \proc_open(['php', '-r', $phpCode], $descriptors, $pipes);

        if (!\is_resource($process)) {
            throw new \RuntimeException('Failed to spawn background notifier process');
        }

        \fclose($pipes[0]);

        $this->backgroundProcesses[] = $process;
        $this->backgroundStderrLogs[] = $stderrLog;
    }
}
