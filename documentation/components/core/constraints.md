# Constraints

- [⬅️️ Back](/documentation/components/core/core.md)

Data constraints allow you to apply business rules and data integrity checks to ensure data quality during processing. When a constraint is violated, a `ConstraintViolationException` is thrown with details about the violating row.

## Unique Constraints

Ensure that values in specified columns are unique across the entire dataset.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, constraint_unique, to_output};

$dataFrame = data_frame()
    ->read(from_array([
        ['email' => 'user1@example.com', 'username' => 'user1'],
        ['email' => 'user2@example.com', 'username' => 'user2'],
        ['email' => 'user1@example.com', 'username' => 'user3'], // This will cause constraint violation
    ]))
    ->constrain(constraint_unique('email'))
    ->write(to_output())
    ->run();
```

## Multiple Column Constraints

Ensure unique combinations across multiple columns:

```php
<?php

use function Flow\ETL\DSL\{constraint_unique};

$dataFrame = data_frame()
    ->read($extractor)
    ->constrain(constraint_unique('username', 'tenant_id'))
    ->write($loader)
    ->run();
```

## Custom Constraints

You can implement custom constraints by creating classes that implement the `Constraint` interface:

```php
<?php

use Flow\ETL\{Constraint, Row};

class AgeRangeConstraint implements Constraint
{
    public function __construct(private int $minAge, private int $maxAge) {}
    
    public function isSatisfiedBy(Row $row): bool
    {
        $age = $row->get('age')->value();
        return $age >= $this->minAge && $age <= $this->maxAge;
    }
    
    public function toString(): string
    {
        return "Age must be between {$this->minAge} and {$this->maxAge}";
    }
    
    public function violation(Row $row): string
    {
        return "Age {$row->get('age')->value()} is outside allowed range";
    }
}
```