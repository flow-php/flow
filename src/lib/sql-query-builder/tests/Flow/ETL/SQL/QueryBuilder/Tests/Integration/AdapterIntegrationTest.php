<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\Adapter\DbalQueryBuilderAdapter;
use Flow\ETL\SQL\QueryBuilder\Adapter\FlowQueryBuilderWrapper;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

class AdapterIntegrationTest extends DatabaseTestCase
{
    protected function createConnection(): Connection
    {
        return DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);
    }

    public function test_dbal_adapter_conversion(): void
    {
        // Create DBAL QueryBuilder
        $dbalQb = $this->connection->createQueryBuilder()
            ->select('u.id', 'u.name', 'COUNT(o.id) as order_count')
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->where('u.status = :status')
            ->groupBy('u.id', 'u.name')
            ->having('COUNT(o.id) > :min_orders')
            ->orderBy('order_count', 'DESC')
            ->setParameter('status', 'active')
            ->setParameter('min_orders', 0);

        // Convert to Flow QueryBuilder
        $flowQb = DbalQueryBuilderAdapter::fromDbalQueryBuilder($dbalQb);

        // Execute both queries
        $dbalResult = $this->connection->executeQuery(
            $dbalQb->getSQL(),
            $dbalQb->getParameters(),
            $dbalQb->getParameterTypes()
        )->fetchAllAssociative();

        $flowResult = $this->connection->executeQuery(
            $flowQb->toSQL(),
            $flowQb->getParameters(),
            $flowQb->getParameterTypes()
        )->fetchAllAssociative();

        // Results should be identical
        self::assertEquals($dbalResult, $flowResult);
        self::assertCount(4, $flowResult); // 4 active users
    }

    public function test_flow_wrapper_compatibility(): void
    {
        $wrapper = new FlowQueryBuilderWrapper($this->connection);

        // Use DBAL-style methods
        $wrapper
            ->select('u.*')
            ->from('users', 'u')
            ->where('u.status = :status')
            ->setParameter('status', 'active')
            ->orderBy('u.name');

        // Add Flow-specific features
        $subqueryBuilder = new FlowQueryBuilder(PlatformFactory::fromConnection($this->connection));
        $subquery = $subqueryBuilder
            ->select('user_id', 'COUNT(*) as order_count')
            ->from('orders')
            ->groupBy('user_id');

        $wrapper->with('user_orders', $subquery);
        
        // Continue with DBAL-style join
        $wrapper->leftJoin('u', 'user_orders', 'uo', 'uo.user_id = u.id');
        $wrapper->addSelect('COALESCE(uo.order_count, 0) as orders');

        $result = $this->connection->executeQuery(
            $wrapper->getSQL(),
            $wrapper->getParameters(),
            $wrapper->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(4, $result);
        foreach ($result as $row) {
            self::assertEquals('active', $row['status']);
            self::assertArrayHasKey('orders', $row);
        }
    }

    public function test_query_parts_access_through_wrapper(): void
    {
        $wrapper = new FlowQueryBuilderWrapper($this->connection);

        $wrapper
            ->select('*')
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->where('u.status = :status')
            ->groupBy('u.id')
            ->orderBy('u.name')
            ->setParameter('status', 'active');

        // Access query parts
        $parts = $wrapper->getQueryParts();

        self::assertArrayHasKey('select', $parts);
        self::assertArrayHasKey('from', $parts);
        self::assertArrayHasKey('joins', $parts);
        self::assertArrayHasKey('where', $parts);
        self::assertArrayHasKey('groupBy', $parts);
        self::assertArrayHasKey('orderBy', $parts);

        // Reset parts
        $wrapper->resetQueryPart('orderBy');
        $wrapper->resetQueryPart('groupBy');

        $newParts = $wrapper->getQueryParts();
        self::assertEmpty($newParts['orderBy']);
        self::assertEmpty($newParts['groupBy']);
    }

    public function test_parameter_handling_across_adapters(): void
    {
        // Create complex DBAL query with parameters
        $dbalQb = $this->connection->createQueryBuilder()
            ->select('*')
            ->from('orders')
            ->where('user_id = :user_id')
            ->andWhere('amount BETWEEN :min_amount AND :max_amount')
            ->andWhere('status IN (:statuses)')
            ->setParameter('user_id', 1)
            ->setParameter('min_amount', 50.0)
            ->setParameter('max_amount', 200.0)
            ->setParameter('statuses', ['completed', 'pending'], Connection::PARAM_STR_ARRAY);

        // Convert to Flow
        $flowQb = DbalQueryBuilderAdapter::fromDbalQueryBuilder($dbalQb);

        // Parameters should be preserved
        $params = $flowQb->getParameters();
        self::assertEquals(1, $params['user_id']);
        self::assertEquals(50.0, $params['min_amount']);
        self::assertEquals(200.0, $params['max_amount']);
        self::assertEquals(['completed', 'pending'], $params['statuses']);

        // Execute query
        $result = $this->connection->executeQuery(
            $flowQb->toSQL(),
            $flowQb->getParameters(),
            $flowQb->getParameterTypes()
        )->fetchAllAssociative();

        foreach ($result as $row) {
            self::assertEquals(1, $row['user_id']);
            self::assertGreaterThanOrEqual(50, $row['amount']);
            self::assertLessThanOrEqual(200, $row['amount']);
            self::assertContains($row['status'], ['completed', 'pending']);
        }
    }

    public function test_cte_through_wrapper(): void
    {
        $wrapper = new FlowQueryBuilderWrapper($this->connection);

        // Create CTE using wrapper
        $highValueUsers = new FlowQueryBuilder(PlatformFactory::fromConnection($this->connection));
        $highValueUsers
            ->select('u.id', 'u.name', 'SUM(o.amount) as total_spent')
            ->from('users', 'u')
            ->join('u', 'orders', 'o', 'o.user_id = u.id')
            ->where('o.status = :status')
            ->groupBy('u.id', 'u.name')
            ->having('SUM(o.amount) > :threshold')
            ->setParameter('status', 'completed')
            ->setParameter('threshold', 200);

        $wrapper
            ->with('high_value_users', $highValueUsers)
            ->select('*')
            ->from('high_value_users')
            ->orderBy('total_spent', 'DESC');

        $result = $this->connection->executeQuery(
            $wrapper->getSQL(),
            $wrapper->getParameters(),
            $wrapper->getParameterTypes()
        )->fetchAllAssociative();

        foreach ($result as $row) {
            self::assertGreaterThan(200, $row['total_spent']);
        }
    }

    public function test_cloning_behavior(): void
    {
        $wrapper = new FlowQueryBuilderWrapper($this->connection);
        
        $wrapper
            ->select('*')
            ->from('users')
            ->where('status = :status')
            ->setParameter('status', 'active');

        // Clone the wrapper
        $clone = clone $wrapper;
        $clone->andWhere('created_at > :date')
            ->setParameter('date', '2024-02-01');

        // Original should not be affected
        $originalParts = $wrapper->getQueryParts();
        $cloneParts = $clone->getQueryParts();

        self::assertCount(1, $originalParts['where']);
        self::assertCount(2, $cloneParts['where']);

        self::assertCount(1, $wrapper->getParameters());
        self::assertCount(2, $clone->getParameters());
    }

    public function test_mixed_builder_usage(): void
    {
        // Start with DBAL
        $dbalQb = $this->connection->createQueryBuilder()
            ->select('u.id', 'u.name')
            ->from('users', 'u')
            ->where('u.status = :status')
            ->setParameter('status', 'active');

        // Convert to Flow for CTE support
        $flowQb = DbalQueryBuilderAdapter::fromDbalQueryBuilder($dbalQb);

        // Create a new Flow query with CTE
        $finalBuilder = new FlowQueryBuilder(PlatformFactory::fromConnection($this->connection));
        
        $finalBuilder
            ->with('active_users', $flowQb)
            ->select('au.*', 'COUNT(o.id) as order_count')
            ->from('active_users', 'au')
            ->leftJoin('au', 'orders', 'o', 'o.user_id = au.id')
            ->groupBy('au.id', 'au.name')
            ->orderBy('order_count', 'DESC');

        $result = $this->connection->executeQuery(
            $finalBuilder->toSQL(),
            $finalBuilder->getParameters(),
            $finalBuilder->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(4, $result); // 4 active users
        foreach ($result as $row) {
            self::assertArrayHasKey('order_count', $row);
        }
    }
}