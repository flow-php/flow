<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use function Flow\PostgreSql\DSL\{and_, col, count_all, delete, eq, insert, is_null, le, or_, order_by, param, select, star, table, typed, update};

use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;

final readonly class Connection
{
    public function __construct(
        private Client $client,
        private string $tableName = 'messenger_messages',
        private string $schemaName = 'public',
        private string $queueName = 'default',
        private int $redeliverTimeout = 3600,
    ) {
    }

    public function ack(string $id) : bool
    {
        return $this->client->execute(
            delete()
                ->from(table($this->tableName, $this->schemaName))
                ->where(eq(col('id'), param(1))),
            [(int) $id],
        ) > 0;
    }

    /**
     * @return null|array<string, mixed>
     */
    public function find(int|string $id) : ?array
    {
        return $this->client->fetch(
            select(star())
                ->from(table($this->tableName, $this->schemaName))
                ->where(and_(
                    eq(col('id'), param(1)),
                    eq(col('queue_name'), param(2)),
                )),
            [(int) $id, $this->queueName],
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function findAll(?int $limit = null) : array
    {
        $query = select(star())
            ->from(table($this->tableName, $this->schemaName))
            ->where(eq(col('queue_name'), param(1)))
            ->orderBy(order_by(col('available_at'), SortDirection::ASC));

        if ($limit !== null) {
            $query = $query->limit($limit);
        }

        return \array_values($this->client->fetchAll($query, [$this->queueName]));
    }

    /**
     * @return null|array<string, mixed>
     */
    public function get() : ?array
    {
        return $this->client->transaction(function (Client $client) : ?array {
            $now = new \DateTimeImmutable('now');
            $redeliverCutoff = $now->sub(new \DateInterval('PT' . $this->redeliverTimeout . 'S'));

            $row = $client->fetch(
                select(star())
                    ->from(table($this->tableName, $this->schemaName))
                    ->where(and_(
                        eq(col('queue_name'), param(1)),
                        le(col('available_at'), param(2)),
                        or_(
                            is_null(col('delivered_at')),
                            le(col('delivered_at'), param(3)),
                        ),
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

            $rowId = $row['id'];

            if (!\is_int($rowId) && !\is_string($rowId)) {
                throw TransportException::unexpectedRowShape('id', \get_debug_type($rowId));
            }

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

    public function getMessageCount() : int
    {
        $now = new \DateTimeImmutable('now');

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

    public function keepalive(string $id, ?int $seconds = null) : void
    {
        if ($seconds !== null && $this->redeliverTimeout < $seconds) {
            throw new TransportException(\sprintf(
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
                typed(new \DateTimeImmutable('now'), ValueType::TIMESTAMPTZ),
                (int) $id,
            ],
        );
    }

    public function reject(string $id) : bool
    {
        return $this->ack($id);
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function send(string $body, array $headers, int $delay = 0) : string
    {
        $now = new \DateTimeImmutable('now');
        $availableAt = $delay > 0
            ? $now->modify(\sprintf('%+d seconds', (int) ($delay / 1000)))
            : $now;

        $row = $this->client->fetchSingle(
            insert()
                ->into(table($this->tableName, $this->schemaName))
                ->columns('body', 'headers', 'queue_name', 'created_at', 'available_at')
                ->values(param(1), param(2), param(3), param(4), param(5))
                ->returning(col('id')),
            [
                $body,
                \json_encode($headers, \JSON_THROW_ON_ERROR),
                $this->queueName,
                typed($now, ValueType::TIMESTAMPTZ),
                typed($availableAt, ValueType::TIMESTAMPTZ),
            ],
        );

        $rowId = $row['id'];

        if (!\is_int($rowId) && !\is_string($rowId)) {
            throw TransportException::unexpectedRowShape('id', \get_debug_type($rowId));
        }

        return (string) $rowId;
    }
}
