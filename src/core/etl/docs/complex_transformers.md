## Complex Transformers 

[<< back](../README.md)

Explanation of complex transformers and their arguments:

### Complex Transformers

Below transformers might not be self descriptive and might require some additional options/dependencies.

#### Transformer - FilterRows

Available Filters

- [all](../src/Flow/ETL/Transformer/Filter/Filter/All.php)
- [any](../src/Flow/ETL/Transformer/Filter/Filter/Any.php)
- [callback](../src/Flow/ETL/Transformer/Filter/Filter/Callback.php)
- [entry equals to](../src/Flow/ETL/Transformer/Filter/Filter/EntryEqualsTo.php)
- [entry not equals to](../src/Flow/ETL/Transformer/Filter/Filter/EntryNotEqualsTo.php)
- [entry not null](../src/Flow/ETL/Transformer/Filter/Filter/EntryNotNull.php)
- [entry not number](../src/Flow/ETL/Transformer/Filter/Filter/EntryNotNumber.php)
- [entry number](../src/Flow/ETL/Transformer/Filter/Filter/EntryNumber.php)
- [entry exists](../src/Flow/ETL/Transformer/Filter/Filter/EntryExists.php)
- [opposite](../src/Flow/ETL/Transformer/Filter/Filter/Opposite.php)
- [valid value](../src/Flow/ETL/Transformer/Filter/Filter/ValidValue.php) - optionally integrates with [symfony validator](https://github.com/symfony/validator)

#### Transformer - Conditional

Transforms only those Rows that met given condition.

Available Conditions

- [all](../src/Flow/ETL/Transformer/Condition/All.php)
- [any](../src/Flow/ETL/Transformer/Condition/Any.php)
- [array dot exists](../src/Flow/ETL/Transformer/Condition/ArrayDotExists.php)
- [array dot value equals to](../src/Flow/ETL/Transformer/Condition/ArrayDotValueEqualsTo.php)
- [array dot value greater or equal than](../src/Flow/ETL/Transformer/Condition/ArrayDotValueGreaterOrEqualThan.php)
- [array dot value greater than](../src/Flow/ETL/Transformer/Condition/ArrayDotValueGreaterThan.php)
- [array dot value less or equal than](../src/Flow/ETL/Transformer/Condition/ArrayDotValueLessOrEqualThan.php)
- [array dot value less than](../src/Flow/ETL/Transformer/Condition/ArrayDotValueLessThan.php)
- [entry exists](../src/Flow/ETL/Transformer/Condition/EntryExists.php)
- [entry instance of](../src/Flow/ETL/Transformer/Condition/EntryInstanceOf.php)
- [entry not null](../src/Flow/ETL/Transformer/Condition/EntryNotNull.php)
- [entry value equals to](../src/Flow/ETL/Transformer/Condition/EntryValueEqualsTo.php)
- [entry value greater or equal than](../src/Flow/ETL/Transformer/Condition/EntryValueGreaterOrEqualThan.php)
- [entry value greater than](../src/Flow/ETL/Transformer/Condition/EntryValueGreaterThan.php)
- [entry value less or equal than](../src/Flow/ETL/Transformer/Condition/EntryValueLessOrEqualThan.php)
- [entry value less than](../src/Flow/ETL/Transformer/Condition/EntryValueLessThan.php)
- [none](../src/Flow/ETL/Transformer/Condition/None.php)
- [opposite](../src/Flow/ETL/Transformer/Condition/Opposite.php)
- [valid value](../src/Flow/ETL/Transformer/Condition/ValidValue) - optionally integrates with [Symfony Validator](https://github.com/symfony/validator)


#### Transformer - Cast


Casting Types:

* [cast entries](../src/Flow/ETL/Transformer/Cast/CastEntries.php)
* [cast array entry each](../src/Flow/ETL/Transformer/Cast/CastArrayEntryEach.php)
* [cast to datetime](../src/Flow/ETL/Transformer/Cast/CastToDateTime.php)
* [cast to string](../src/Flow/ETL/Transformer/Cast/CastToString.php)
* [cast to integer](../src/Flow/ETL/Transformer/Cast/CastToInteger.php)
* [cast to float](../src/Flow/ETL/Transformer/Cast/CastToFloat.php)
* [cast to json](../src/Flow/ETL/Transformer/Cast/CastToJson.php)
* [cast to array](../src/Flow/ETL/Transformer/Cast/CastToArray.php)
* [cast json to array](../src/Flow/ETL/Transformer/Cast/CastJsonToArray.php)

#### Transformer - EntryNameStyleConverter

Available styles: 

* `camel`
* `pascal`
* `snake`
* `ada`
* `macro`
* `kebab`
* `train`
* `cobol`
* `lower`
* `upper`
* `title`
* `sentence`
* `dot`


[<< back](../README.md)