<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use DateInterval;
use DateTimeImmutable;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;

use function array_values;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\count_all;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\is_null;
use function Flow\PostgreSql\DSL\le;
use function Flow\PostgreSql\DSL\or_;
use function Flow\PostgreSql\DSL\order_by;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\typed;
use function Flow\PostgreSql\DSL\update;
use function get_debug_type;
use function is_int;
use function is_string;
use function json_encode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final readonly class Connection
{
    public function __construct(
        private Client $client,
        private string $tableName = 'messenger_messages',
        private string $schemaName = 'public',
        private string $queueName = 'default',
        private int $redeliverTimeout = 3600,
    ) {}

    public function ack(string $id): bool
    {
        return $this->client->execute(
            delete()->from(table($this->tableName, $this->schemaName))->where(eq(col('id'), param(1))),
            [(int) $id],
        ) > 0;
    }

    /**
     * @return null|array<string, mixed>
     */
    public function find(int|string $id): ?array
    {
        return $this->client->fetch(
            select(star())
                ->from(table($this->tableName, $this->schemaName))
                ->where(and_(eq(col('id'), param(1)), eq(col('queue_name'), param(2)))),
            [(int) $id, $this->queueName],
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function findAll(?int $limit = null): array
    {
        $query = select(star())
            ->from(table($this->tableName, $this->schemaName))
            ->where(eq(col('queue_name'), param(1)))
            ->orderBy(order_by(col('available_at'), SortDirection::ASC));

        if ($limit !== null) {
            $query = $query->limit($limit);
        }

        return array_values($this->client->fetchAll($query, [$this->queueName]));
    }

    /**
     * @return null|array<string, mixed>
     */
    public function get(): ?array
    {
        return $this->client->transaction(function (Client $client): ?array {
            $now = new DateTimeImmutable('now');
            $redeliverCutoff = $now->sub(new DateInterval('PT' . $this->redeliverTimeout . 'S'));

            $row = $client->fetch(
                select(star())
                    ->from(table($this->tableName, $this->schemaName))
                    ->where(and_(
                        eq(col('queue_name'), param(1)),
                        le(col('available_at'), param(2)),
                        or_(is_null(col('delivered_at')), le(col('delivered_at'), param(3))),
                    ))
                    ->orderBy(order_by(col('available_at'), SortDirection::ASC))
                    ->limit(1)
                    ->forUpdateSkipLocked(),
                [
                    $this->queueName,
                    typed($now, ValueType::TIMESTAMPTZ),
                    typed($redeliverCutoff, ValueType::TIMESTAMPTZ),
                ],
            );

            if ($row === null) {
                return null;
            }

            $rowId = is_int($row['id'] ?? null)
                ? (int) $row['id']
                : (
                    is_string($row['id'] ?? null)
                        ? $row['id']
                        : throw TransportException::unexpectedRowShape('id', get_debug_type($row['id'] ?? null))
                );

            $client->execute(
                update()
                    ->update(table($this->tableName, $this->schemaName))
                    ->set('delivered_at', param(1))
                    ->where(eq(col('id'), param(2))),
                [
                    typed($now, ValueType::TIMESTAMPTZ),
                    (int) $rowId,
                ],
            );

            return $row;
        });
    }

    public function getMessageCount(): int
    {
        $now = new DateTimeImmutable('now');

        return $this->client->fetchScalarInt(
            select(count_all())
                ->from(table($this->tableName, $this->schemaName))
                ->where(and_(
                    eq(col('queue_name'), param(1)),
                    le(col('available_at'), param(2)),
                    is_null(col('delivered_at')),
                )),
            [
                $this->queueName,
                typed($now, ValueType::TIMESTAMPTZ),
            ],
        );
    }

    public function keepalive(string $id, ?int $seconds = null): void
    {
        if ($seconds !== null && $this->redeliverTimeout < $seconds) {
            throw new TransportException(sprintf(
                'Flow PostgreSQL Messenger redeliver_timeout (%ds) cannot be smaller than the keepalive interval (%ds).',
                $this->redeliverTimeout,
                $seconds,
            ));
        }

        $this->client->execute(
            update()
                ->update(table($this->tableName, $this->schemaName))
                ->set('delivered_at', param(1))
                ->where(eq(col('id'), param(2))),
            [
                typed(new DateTimeImmutable('now'), ValueType::TIMESTAMPTZ),
                (int) $id,
            ],
        );
    }

    public function reject(string $id): bool
    {
        return $this->ack($id);
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function send(string $body, array $headers, int $delay = 0): string
    {
        $now = new DateTimeImmutable('now');
        $availableAt = $delay > 0 ? $now->modify(sprintf('%+d seconds', (int) ($delay / 1000))) : $now;

        $row = $this->client->fetchSingle(
            insert()
                ->into(table($this->tableName, $this->schemaName))
                ->columns('body', 'headers', 'queue_name', 'created_at', 'available_at')
                ->values(param(1), param(2), param(3), param(4), param(5))
                ->returning(col('id')),
            [
                $body,
                json_encode($headers, JSON_THROW_ON_ERROR),
                $this->queueName,
                typed($now, ValueType::TIMESTAMPTZ),
                typed($availableAt, ValueType::TIMESTAMPTZ),
            ],
        );

        $rowId = is_int($row['id'] ?? null)
            ? $row['id']
            : (
                is_string($row['id'] ?? null)
                    ? $row['id']
                    : throw TransportException::unexpectedRowShape('id', get_debug_type($row['id'] ?? null))
            );

        return (string) $rowId;
    }
}
