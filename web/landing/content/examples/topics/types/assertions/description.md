Type assertions validate values at runtime and throw `InvalidArgumentException` if the value doesn't match the expected type.  
The `assert()` method returns the validated value with proper type narrowing, enabling static analysis tools like  
PHPStan to understand the resulting type. This provides IDE autocompletion and catches type errors during analysis.  


```php
<?php

$value = takeFromUser(); // $value is type mixed here

$stringValue = type_string()->assert($value); 

// $valueString is type string here, it's called types narrowing 

```

Read more about [types narrowing](https://phpstan.org/writing-php-code/narrowing-types)

