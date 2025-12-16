# Transactions

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The client provides a transaction callback pattern that handles commit and rollback automatically, ensuring data
consistency even when exceptions occur.

## Transaction Callback

The `transaction()` method wraps operations in a transaction:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$result = $client->transaction(function ($client) {
    $client->execute(
        'INSERT INTO accounts (name, balance) VALUES ($1, $2)',
        ['Savings', 1000.00]
    );

    $client->execute(
        'INSERT INTO transactions (account_id, amount) VALUES (lastval(), $1)',
        [1000.00]
    );

    return $client->fetchScalar('SELECT lastval()');
});

echo "Created account: {$result}";
```

## Automatic Commit/Rollback

- **Success**: Transaction commits when the callback returns without throwing
- **Failure**: Transaction rolls back when the callback throws any exception

```php
<?php

try {
    $client->transaction(function ($client) {
        $client->execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]);
        $client->execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]);

        // Validate business rule
        $balance = $client->fetchScalar('SELECT balance FROM accounts WHERE id = $1', [1]);
        if ($balance < 0) {
            throw new \RuntimeException('Insufficient funds');
        }
    });
    echo "Transfer completed";
} catch (\RuntimeException $e) {
    // Transaction was rolled back
    echo "Transfer failed: {$e->getMessage()}";
}
```

## Return Values

The transaction callback can return values:

```php
<?php

// Return inserted ID
$userId = $client->transaction(function ($client) {
    return $client->fetchScalar(
        'INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id',
        ['John', 'john@example.com']
    );
});

// Return multiple values
[$user, $settings] = $client->transaction(function ($client) {
    $user = $client->fetch(
        'INSERT INTO users (name) VALUES ($1) RETURNING *',
        ['John']
    );

    $settings = $client->fetch(
        'INSERT INTO user_settings (user_id) VALUES ($1) RETURNING *',
        [$user['id']]
    );

    return [$user, $settings];
});
```

## Nested Transactions

Nested calls to `transaction()` use SAVEPOINTs:

```php
<?php

$client->transaction(function ($client) {
    $client->execute('INSERT INTO orders (status) VALUES ($1)', ['pending']);

    try {
        // Creates SAVEPOINT
        $client->transaction(function ($client) {
            $client->execute('INSERT INTO order_items (order_id, product_id) VALUES (lastval(), $1)', [1]);

            // This rolls back to SAVEPOINT, not entire transaction
            throw new \RuntimeException('Item not available');
        });
    } catch (\RuntimeException $e) {
        // Outer transaction continues
        $client->execute('UPDATE orders SET status = $1 WHERE id = lastval()', ['partial']);
    }

    // Outer transaction commits
});
```

## Manual Transaction Control

For advanced scenarios, use explicit begin/commit/rollback:

```php
<?php

$client->beginTransaction();

try {
    $client->execute('INSERT INTO users (name) VALUES ($1)', ['John']);
    $client->execute('INSERT INTO profiles (user_id) VALUES (lastval())');

    $client->commit();
} catch (\Throwable $e) {
    $client->rollBack();
    throw $e;
}
```

### Checking Transaction State

```php
<?php

// Get nesting level (0 = no transaction, 1+ = in transaction)
$level = $client->getTransactionNestingLevel();

if ($level > 0) {
    echo "Inside transaction at level {$level}";
}
```

## Auto-Commit Mode

By default, each query runs in its own implicit transaction (auto-commit enabled). You can disable this:

```php
<?php

// Check current mode
if ($client->isAutoCommit()) {
    echo "Auto-commit is enabled";
}

// Disable auto-commit
$client->setAutoCommit(false);

// Now queries don't auto-commit - you must explicitly commit
$client->execute('INSERT INTO logs (message) VALUES ($1)', ['Event 1']);
$client->execute('INSERT INTO logs (message) VALUES ($1)', ['Event 2']);
$client->commit();  // Both inserts committed together

// Re-enable auto-commit
$client->setAutoCommit(true);
```

## Error Handling

Transaction-related exceptions:

```php
<?php

use Flow\PostgreSql\Client\Exception\TransactionException;

try {
    $client->transaction(function ($client) {
        // Operations...
    });
} catch (TransactionException $e) {
    // Transaction failed (deadlock, serialization failure, etc.)
    echo "Transaction error: {$e->getMessage()}";
}
```

## Best Practices

1. **Keep transactions short**: Long transactions hold locks and reduce concurrency
2. **Use the callback pattern**: Ensures proper cleanup on both success and failure
3. **Don't catch and suppress**: Let exceptions propagate for proper rollback
4. **Avoid user interaction**: Never wait for user input inside a transaction

```php
<?php

// GOOD: Short, focused transaction
$client->transaction(function ($client) {
    $client->execute('UPDATE inventory SET quantity = quantity - $1 WHERE id = $2', [1, $productId]);
    $client->execute('INSERT INTO orders (product_id) VALUES ($1)', [$productId]);
});

// BAD: Transaction held while doing external work
$client->transaction(function ($client) use ($emailService) {
    $client->execute('INSERT INTO orders (product_id) VALUES ($1)', [$productId]);
    $emailService->sendConfirmation();  // Don't do this inside transaction!
});
```
